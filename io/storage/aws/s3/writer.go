// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/io/storage/common/pool"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	s3DefaultChunkSize         = 5 * 1024 * 1024 // 5MB, minimum size of a part
	s3DefaultChecksumAlgorithm = types.ChecksumAlgorithmCrc64nvme
)

// Writer represents a s3 storage writer.
type Writer struct {
	// Optional parameters.
	options.Options

	client *s3.Client
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
	// Sync for running backup to one file.
	called atomic.Bool

	storageClass types.StorageClass
}

// NewWriter creates a new writer for S3 storage directory/file writes.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithRemoveFiles() - optional.
// For S3 client next parameters must be set:
//   - o.UsePathStyle = true
//   - o.BaseEndpoint = &endpoint - if endpoint != ""
func NewWriter(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	opts ...options.Opt,
) (*Writer, error) {
	w := &Writer{}

	for _, opt := range opts {
		opt(&w.Options)
	}

	if len(w.PathList) != 1 {
		return nil, fmt.Errorf("one path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if w.ChunkSize < 0 {
		return nil, fmt.Errorf("chunk size must be positive")
	}

	if w.ChunkSize == 0 {
		w.ChunkSize = s3DefaultChunkSize
	}

	if w.IsDir {
		w.prefix = common.CleanPath(w.PathList[0], true)
	}

	// Check if the bucket exists and we have permissions.
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("bucket %s does not exist or you don't have access: %w", bucketName, err)
	}

	if w.IsDir && !w.SkipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(ctx, client, bucketName, w.prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to check if the directory is empty: %w", err)
		}

		if !isEmpty && !w.IsRemovingFiles {
			return nil, fmt.Errorf("backup folder must be empty or set RemoveFiles = true")
		}
	}

	w.client = client
	w.bucketName = bucketName

	if w.IsRemovingFiles {
		err = w.RemoveFiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to delete files under prefix %s: %w", w.prefix, err)
		}
	}

	if w.StorageClass != "" {
		// validation.
		class, err := parseStorageClass(w.StorageClass)
		if err != nil {
			return nil, fmt.Errorf("failed to parse storage class: %w", err)
		}

		w.storageClass = class
	}

	return w, nil
}

// NewWriter returns a new S3 writer to the specified path.
// isRecords indicates whether the file contains record data.
func (w *Writer) NewWriter(ctx context.Context, filename string, isRecords bool) (io.WriteCloser, error) {
	// protection for single file backup.
	if err := common.RestrictParallelBackup(&w.called, w.IsDir, isRecords); err != nil {
		return nil, err
	}

	fullPath, err := common.GetFullPath(w.prefix, filename, w.PathList, w.IsDir, isRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to get full path: %w", err)
	}

	upload, err := w.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            &w.bucketName,
		Key:               &fullPath,
		StorageClass:      w.storageClass,
		ChecksumAlgorithm: s3DefaultChecksumAlgorithm,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	return &s3Writer{
		ctx:         ctx,
		cancel:      cancel,
		uploadID:    upload.UploadId,
		key:         fullPath,
		client:      w.client,
		bucket:      w.bucketName,
		buffer:      new(bytes.Buffer),
		chunkSize:   w.ChunkSize,
		logger:      w.Logger,
		retryPolicy: w.RetryPolicy,
		workersPool: pool.NewPool(w.UploadConcurrency),
	}, nil
}

// GetType returns the type of the writer.
func (w *Writer) GetType() string {
	return s3type
}

// s3Writer wrapper for writing files, as S3 in not supporting creation of io.Writer.
type s3Writer struct {
	// ctx is stored internally so that it can be used in io.WriteCloser methods
	ctx    context.Context
	cancel context.CancelFunc

	uploadID *string
	client   *s3.Client
	buffer   *bytes.Buffer
	key      string
	bucket   string

	cpMu           sync.Mutex
	completedParts []types.CompletedPart

	chunkSize   int
	partNumber  atomic.Int32
	closed      atomic.Bool
	logger      *slog.Logger
	retryPolicy *models.RetryPolicy
	workersPool *pool.Pool
	uploadErr   atomic.Value
}

var _ io.WriteCloser = (*s3Writer)(nil)

func (w *s3Writer) Write(p []byte) (int, error) {
	if w.closed.Load() {
		return 0, os.ErrClosed
	}

	if err := w.uploadErr.Load(); err != nil {
		return 0, err.(error)
	}

	if w.buffer.Len() >= w.chunkSize {
		partNumber := w.partNumber.Add(1)

		buf := w.buffer.Bytes()
		// Upload part in a separate goroutine.
		w.workersPool.Submit(func() {
			w.uploadPart(buf, partNumber)
		})
		// Reset buffer for the next chunk.
		w.buffer = new(bytes.Buffer)
	}

	return w.buffer.Write(p)
}

func (w *s3Writer) uploadPart(p []byte, partNumber int32) {
	if w.ctx.Err() != nil || w.uploadErr.Load() != nil {
		return
	}

	var response *s3.UploadPartOutput

	err := w.retryPolicy.Do(w.ctx, func() error {
		var uploadErr error

		response, uploadErr = w.client.UploadPart(w.ctx, &s3.UploadPartInput{
			Body:              bytes.NewReader(p),
			Bucket:            &w.bucket,
			Key:               &w.key,
			PartNumber:        &partNumber,
			UploadId:          w.uploadID,
			ChecksumAlgorithm: s3DefaultChecksumAlgorithm,
		})

		return uploadErr
	})
	if err != nil {
		if w.uploadErr.CompareAndSwap(nil, fmt.Errorf("failed to upload part %d: %w", partNumber, err)) {
			w.cancel()
		}

		return
	}

	w.cpMu.Lock()
	w.completedParts = append(w.completedParts, types.CompletedPart{
		PartNumber: &partNumber,
		ETag:       response.ETag,
		// Fill checksums from response.
		ChecksumCRC64NVME: response.ChecksumCRC64NVME,
	})
	w.cpMu.Unlock()
}

func (w *s3Writer) Close() error {
	if w.closed.Load() {
		return os.ErrClosed
	}

	if w.buffer.Len() > 0 {
		partNumber := w.partNumber.Add(1)

		lastPart := w.buffer.Bytes()

		w.uploadPart(lastPart, partNumber)
	}
	// Wait for all workers to finish.
	w.workersPool.Wait()

	if err := w.uploadErr.Load(); err != nil {
		return err.(error)
	}

	// Sort completed parts by part number (required by S3).
	w.cpMu.Lock()
	sort.Slice(w.completedParts, func(i, j int) bool {
		return *w.completedParts[i].PartNumber < *w.completedParts[j].PartNumber
	})

	// Verify no gaps in part numbers.
	for i, part := range w.completedParts {
		expectedPartNum := int32(i + 1)
		if *part.PartNumber != expectedPartNum {
			if err := w.abortUpload(fmt.Errorf("missing part %d in upload sequence", expectedPartNum)); err != nil {
				if w.logger != nil {
					w.logger.Error("failed to abort multipart upload",
						slog.String("key", w.key),
						slog.String("uploadID", *w.uploadID),
						slog.Any("error", err))
				}

				return err
			}
		}
	}

	r, err := w.client.CompleteMultipartUpload(w.ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:   &w.bucket,
			UploadId: w.uploadID,
			Key:      &w.key,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: w.completedParts,
			},
		})
	if err != nil {
		// Try to abort even if complete upload failed.
		if abErr := w.abortUpload(err); abErr != nil {
			if w.logger != nil {
				w.logger.Error("failed to abort multipart upload",
					slog.String("key", w.key),
					slog.String("uploadID", *w.uploadID),
					slog.Any("error", err))
			}
		}

		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	if w.logger != nil {
		w.logger.Debug("completed multipart upload",
			slog.String("key", w.key),
			slog.String("uploadID", *w.uploadID),
			slog.Int("parts", len(w.completedParts)),
			slog.String("etag", *r.ETag),
			slog.String("checksum", *r.ChecksumCRC64NVME),
		)
	}

	w.cpMu.Unlock()

	w.closed.Store(true)

	return nil
}

// abortUpload aborts the multipart upload and cleans up partial data
func (w *s3Writer) abortUpload(originalErr error) error {
	// Use a fresh context for cleanup (not the cancelled one).
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := w.client.AbortMultipartUpload(cleanupCtx, &s3.AbortMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: w.uploadID,
	})

	return errors.Join(originalErr, err)
}

func isEmptyDirectory(ctx context.Context, client *s3.Client, bucketName, prefix string) (bool, error) {
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &bucketName,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, fmt.Errorf("failed to list bucket objects: %w", err)
	}

	// Check if it's a single object
	if len(resp.Contents) == 1 && *resp.Contents[0].Key == prefix {
		return false, nil
	}

	return len(resp.Contents) == 0, nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	return w.Remove(ctx, w.PathList[0])
}

// Remove deletes the file or directory contents specified by path.
func (w *Writer) Remove(ctx context.Context, targetPath string) error {
	// Remove file.
	if !w.IsDir {
		if _, err := w.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(w.bucketName),
			Key:    aws.String(targetPath),
		}); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", targetPath, err)
		}

		return nil
	}
	// Remove files from dir.
	var continuationToken *string

	prefix := common.CleanPath(targetPath, true)

	for {
		listResponse, err := w.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(w.bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if p.Key == nil || common.IsDirectory(prefix, *p.Key) && !w.WithNestedDir {
				continue
			}

			// If validator is set, remove only valid files.
			if w.Validator != nil {
				if err = w.Validator.Run(*p.Key); err != nil {
					continue
				}
			}

			_, err = w.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: &w.bucketName,
				Key:    p.Key,
			})
			if err != nil {
				return fmt.Errorf("failed to delete object %s: %w", *p.Key, err)
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return nil
}

func parseStorageClass(class string) (types.StorageClass, error) {
	// To correct case: CLASS
	class = strings.ToUpper(class)

	var result types.StorageClass
	possible := result.Values()

	for _, possibleClass := range possible {
		if class == string(possibleClass) {
			result = possibleClass
			break
		}
	}

	if result == "" {
		return "", fmt.Errorf("invalid storage class %s", class)
	}

	return result, nil
}
