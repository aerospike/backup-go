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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/ptr"
)

const (
	s3DefaultChunkSize = 5 * 1024 * 1024 // 5MB, minimum size of a part (S3 multipart minimum)
	// maxDeleteObjectsPerRequest is the S3 DeleteObjects limit per API call.
	maxDeleteObjectsPerRequest = 1000
)

// Writer configures backup uploads to S3. It validates the bucket and optional prefix,
// then builds a [transfermanager.Client] used for each opened object.
//
// Uploads are streaming: each call to (*Writer).NewWriter runs an S3 upload in a
// background goroutine while the caller writes to an in-memory pipe. The transfer
// manager chooses PutObject vs multipart upload from the stream shape and options
// (see NewWriter), so unlike a hand-rolled multipart flow it does not call
// CreateMultipartUpload until the first full part is read from the body.
type Writer struct {
	// Optional parameters.
	options.Options

	client Client
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string

	// storageClass is set when options.StorageClass is non-empty; validated in NewWriter.
	storageClass tmtypes.StorageClass
	uploader     *transfermanager.Client
}

// NewWriter creates a new writer for S3 storage directory/file writes.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithRemoveFiles() - optional.
// For S3 client next parameters must be set:
//   - o.UsePathStyle = true
//   - o.BaseEndpoint = &endpoint - if endpoint != ""
func NewWriter(
	ctx context.Context,
	client Client,
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
		class, err := parseStorageClass(w.StorageClass)
		if err != nil {
			return nil, fmt.Errorf("failed to parse storage class: %w", err)
		}

		w.storageClass = class
	}

	// Part size is floored at 5MB so we never ask S3 for multipart parts below its minimum.
	// MultipartUploadThreshold matches part size so objects that exceed one part use multipart
	// rather than buffering to a higher threshold first.
	partSize := int64(max(w.ChunkSize, s3DefaultChunkSize))

	uploadConcurrency := w.UploadConcurrency
	if uploadConcurrency <= 0 {
		uploadConcurrency = 1
	}

	w.uploader = transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = partSize
		o.MultipartUploadThreshold = partSize
		o.Concurrency = uploadConcurrency
	})

	return w, nil
}

// NewWriter opens an object key for write. The returned [io.WriteCloser] writes to a pipe;
// a goroutine reads the other end and runs [transfermanager.Client.UploadObject], which may
// use PutObject (small/unknown-length streams that finish under one part) or multipart upload.
// Errors from that upload are delivered asynchronously: first non-blocking delivery on a
// successful Write after the upload finishes (tryCollectUploadResult), or on Close (waitForUploadResult).
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	fullPath, err := common.GetFullPath(w.prefix, filename, w.PathList, w.IsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get full path: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()

	s3w := &s3Writer{
		ctx:        ctx,
		cancel:     cancel,
		pipeWriter: pw,
		uploadDone: make(chan error, 1),
	}

	uploadInput := &transfermanager.UploadObjectInput{
		Bucket: &w.bucketName,
		Key:    &fullPath,
		Body:   pr,
	}
	if w.storageClass != "" {
		uploadInput.StorageClass = w.storageClass
	}

	if w.WithChecksum {
		uploadInput.ChecksumAlgorithm = tmtypes.ChecksumAlgorithmCrc32
	}

	go func() {
		defer func() {
			_ = pr.Close()
		}()

		_, uploadErr := w.uploader.UploadObject(ctx, uploadInput)
		if uploadErr != nil {
			// Unblocks Write() immediately if upload fails while caller is still writing.
			_ = pw.CloseWithError(uploadErr)
		}

		s3w.uploadDone <- uploadErr
	}()

	return s3w, nil
}

// GetType returns the type of the writer.
func (w *Writer) GetType() string {
	return s3type
}

// s3Writer implements [io.WriteCloser] over an [io.PipeWriter]. The read side of the pipe is
// consumed by the transfer manager upload goroutine; writeMu serializes Write/Close with the
// pipe’s close semantics. uploadDone carries the final error from UploadObject exactly once;
// tryCollectUploadResult drains it without blocking when a Write runs after the upload completed.
type s3Writer struct {
	ctx    context.Context
	cancel context.CancelFunc

	writeMu sync.Mutex

	pipeWriter *io.PipeWriter
	uploadDone chan error
	uploadErr  atomic.Value // stores error; only non-nil after upload finishes
	closed     atomic.Bool
}

var _ io.WriteCloser = (*s3Writer)(nil)

func (w *s3Writer) Write(p []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.closed.Load() {
		return 0, os.ErrClosed
	}

	if err := w.uploadErr.Load(); err != nil {
		return 0, err.(error)
	}

	if len(p) == 0 {
		return 0, nil
	}

	n, err := w.pipeWriter.Write(p)
	if err != nil {
		if uploadErr := w.loadUploadErr(); uploadErr != nil {
			return n, uploadErr
		}

		return n, err
	}

	// If the upload goroutine already finished, surface its error without blocking this Write.
	if err := w.tryCollectUploadResult(); err != nil {
		return n, err
	}

	return n, nil
}

func (w *s3Writer) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return os.ErrClosed
	}
	defer w.cancel()

	w.writeMu.Lock()
	closeErr := w.pipeWriter.Close() // unblocks the reader; remaining body is EOF
	w.writeMu.Unlock()

	uploadErr := w.waitForUploadResult()
	if uploadErr != nil {
		return uploadErr
	}

	if closeErr != nil && !errors.Is(closeErr, io.ErrClosedPipe) {
		return closeErr
	}

	return nil
}

func (w *s3Writer) loadUploadErr() error {
	if err := w.uploadErr.Load(); err != nil {
		return err.(error)
	}

	return nil
}

func (w *s3Writer) setUploadErr(err error) {
	if err == nil {
		return
	}

	w.uploadErr.CompareAndSwap(nil, err)
}

// tryCollectUploadResult receives the upload outcome if it is already available; otherwise it is a no-op.
func (w *s3Writer) tryCollectUploadResult() error {
	if w.uploadDone == nil {
		return w.loadUploadErr()
	}

	select {
	case err := <-w.uploadDone:
		w.uploadDone = nil
		w.setUploadErr(err)
	default:
	}

	return w.loadUploadErr()
}

// waitForUploadResult blocks until the upload goroutine sends its result (normally on Close).
func (w *s3Writer) waitForUploadResult() error {
	if w.uploadDone != nil {
		err := <-w.uploadDone
		w.uploadDone = nil
		w.setUploadErr(err)
	}

	return w.loadUploadErr()
}

func isEmptyDirectory(ctx context.Context, client Client, bucketName, prefix string) (bool, error) {
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
	// Remove files from dir using batch DeleteObjects (up to maxDeleteObjectsPerRequest per call).
	var continuationToken *string

	prefix := common.CleanPath(targetPath, true)
	batch := make([]string, 0, maxDeleteObjectsPerRequest)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := w.deleteObjectsBatch(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]

		return nil
	}

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
			if p.Key == nil || common.IsDirectory(prefix, ptr.ToString(p.Key)) && !w.WithNestedDir {
				continue
			}

			key := ptr.ToString(p.Key)
			if w.Validator != nil {
				if err = w.Validator.Run(key); err != nil {
					continue
				}
			}

			batch = append(batch, key)
			if len(batch) >= maxDeleteObjectsPerRequest {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return flushBatch()
}

func (w *Writer) deleteObjectsBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	objs := make([]awstypes.ObjectIdentifier, len(keys))
	for i := range keys {
		objs[i] = awstypes.ObjectIdentifier{Key: aws.String(keys[i])}
	}

	out, err := w.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(w.bucketName),
		Delete: &awstypes.Delete{
			Objects: objs,
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete objects: %w", err)
	}

	if len(out.Errors) > 0 {
		return fmt.Errorf("failed to delete some objects: %s", formatDeleteObjectsErrors(out.Errors))
	}

	return nil
}

func formatDeleteObjectsErrors(errs []awstypes.Error) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		key := aws.ToString(e.Key)

		msg := aws.ToString(e.Message)
		if msg == "" {
			msg = aws.ToString(e.Code)
		}
		parts = append(parts, fmt.Sprintf("%s: %s", key, msg))
	}

	return strings.Join(parts, "; ")
}

// knownStorageClasses is the set accepted for uploads; it matches transfer manager / S3 PutObject enums.
var knownStorageClasses = []tmtypes.StorageClass{
	tmtypes.StorageClassStandard,
	tmtypes.StorageClassReducedRedundancy,
	tmtypes.StorageClassStandardIa,
	tmtypes.StorageClassOnezoneIa,
	tmtypes.StorageClassIntelligentTiering,
	tmtypes.StorageClassGlacier,
	tmtypes.StorageClassDeepArchive,
	tmtypes.StorageClassOutposts,
	tmtypes.StorageClassGlacierIr,
	tmtypes.StorageClassSnow,
	tmtypes.StorageClassExpressOnezone,
}

// parseStorageClass normalizes user input and returns a transfer-manager storage class value.
func parseStorageClass(class string) (tmtypes.StorageClass, error) {
	class = strings.ToUpper(strings.TrimSpace(class))

	for _, v := range knownStorageClasses {
		if class == string(v) {
			return v, nil
		}
	}

	if class == "" {
		return "", fmt.Errorf("invalid storage class %s", class)
	}

	return "", fmt.Errorf("invalid storage class %s", class)
}

// GetOptions returns initialized options for the writer.
func (w *Writer) GetOptions() options.Options {
	return w.Options
}
