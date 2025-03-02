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
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsHttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const s3type = "s3"

// Reader represents S3 storage reader.
type Reader struct {
	// Optional parameters.
	ioStorage.Options

	client *s3.Client

	// bucketName contains the name of the bucket to read from.
	bucketName string

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string

	// objectsToWarm is used to track the current number of restoring objects.
	objectsToWarm map[string]struct{}
}

// NewReader returns new S3 storage reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
// For S3 client next parameters must be set:
//   - o.UsePathStyle = true
//   - o.BaseEndpoint = &endpoint - if endpoint != ""
func NewReader(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	opts ...ioStorage.Opt,
) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.Options)
	}

	if len(r.PathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	// Check if the bucket exists and we have permissions.
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		return nil, fmt.Errorf("bucket %s does not exist or you don't have access: %w", bucketName, err)
	}

	r.client = client
	r.bucketName = bucketName

	if r.IsDir {
		if !r.SkipDirCheck {
			if err := r.checkRestoreDirectory(ctx, r.PathList[0]); err != nil {
				return nil, fmt.Errorf("%w: %w", ioStorage.ErrEmptyStorage, err)
			}
		}

		// Presort files if needed.
		if r.SortFiles && len(r.PathList) == 1 {
			if err := ioStorage.PreSort(ctx, r, r.PathList[0]); err != nil {
				return nil, fmt.Errorf("failed to pre sort: %w", err)
			}
		}
	}

	if r.RestoreTier != "" {
		r.objectsToWarm = make(map[string]struct{})

		if err := r.warmStorage(ctx); err != nil {
			return nil, fmt.Errorf("failed to heat the storage: %w", err)
		}
	}

	return r, nil
}

// StreamFiles read files form s3 and send io.Readers to `readersCh` communication chan for lazy loading.
// In case of error, we send error to `errorsCh` channel.
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error,
) {
	defer close(readersCh)

	// If objects were preloaded, we stream them.
	if len(r.objectsToStream) > 0 {
		r.streamSetObjects(ctx, readersCh, errorsCh)
		return
	}

	for _, path := range r.PathList {
		// If it is a folder, open and return.
		switch r.IsDir {
		case true:
			path = ioStorage.CleanPath(path, true)
			if !r.SkipDirCheck {
				err := r.checkRestoreDirectory(ctx, path)
				if err != nil {
					errorsCh <- err
					return
				}
			}

			r.streamDirectory(ctx, path, readersCh, errorsCh)
		case false:
			// If not a folder, only file.
			r.StreamFile(ctx, path, readersCh, errorsCh)
		}
	}
}

// streamDirectory reads directory form s3 and send io.Readers to `readersCh` communication chan for lazy loading.
// In case of error, we send error to `errorsCh` channel.
func (r *Reader) streamDirectory(
	ctx context.Context, path string, readersCh chan<- models.File, errorsCh chan<- error,
) {
	var continuationToken *string

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &path,
			ContinuationToken: continuationToken,
			StartAfter:        &r.StartAfter,
		})

		if err != nil {
			errorsCh <- fmt.Errorf("failed to list objects: %w", err)
			return
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
				continue
			}

			// Skip not valid files if validator is set.
			if r.Validator != nil {
				if err = r.Validator.Run(*p.Key); err != nil {
					// Since we are passing invalid files, we don't need to handle this
					// error and write a test for it. Maybe we should log this information
					// for the user, so they know what is going on.
					continue
				}
			}

			r.openObject(ctx, *p.Key, readersCh, errorsCh, true)
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}
}

// openObject creates object readers and sends them readersCh.
func (r *Reader) openObject(
	ctx context.Context,
	path string,
	readersCh chan<- models.File,
	errorsCh chan<- error,
	skipNotFound bool,
) {
	ok, err := r.checkObjectAvailability(ctx, path)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to check object availability: %w", err)
		return
	}

	if !ok {
		errorsCh <- fmt.Errorf("%w: %s", ioStorage.ErrArchivedObject, path)
		return
	}

	object, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.bucketName,
		Key:    &path,
	})

	if err != nil {
		// Skip 404 not found error.
		var opErr *smithy.OperationError
		if errors.As(err, &opErr) {
			var httpErr *awsHttp.ResponseError
			if errors.As(opErr.Err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound && skipNotFound {
				return
			}
		}

		// We check *p.Key == nil in the beginning.
		errorsCh <- fmt.Errorf("failed to open file %s: %w", path, err)

		return
	}

	if object != nil {
		readersCh <- models.File{Reader: object.Body, Name: filepath.Base(path)}
	}
}

// StreamFile opens single file from s3 and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	// This condition will be true, only if we initialized reader for directory and then want to read
	// a specific file. It is used for state file and by asb service. So it must be initialized with only
	// one path.
	if r.IsDir {
		if len(r.PathList) != 1 {
			errorsCh <- fmt.Errorf("reader must be initialized with only one path")
			return
		}

		filename = filepath.Join(r.PathList[0], filename)
	}

	r.openObject(ctx, filename, readersCh, errorsCh, false)
}

// GetType return `s3type` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return s3type
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context, path string) error {
	var continuationToken *string

	if path == "/" {
		path = ""
	}

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &path,
			ContinuationToken: continuationToken,
			StartAfter:        &r.StartAfter,
		})

		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
				continue
			}

			switch {
			case r.Validator != nil:
				// If we found a valid file, return.
				if err = r.Validator.Run(*p.Key); err == nil {
					return nil
				}
			default:
				// If we found anything, then folder is not empty.
				if p.Key != nil {
					return nil
				}
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return fmt.Errorf("%s is empty", path)
}

// ListObjects list all object in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	var continuationToken *string

	result := make([]string, 0)

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &path,
			ContinuationToken: continuationToken,
			StartAfter:        &r.StartAfter,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
				continue
			}

			if p.Key != nil {
				if r.Validator != nil {
					if err = r.Validator.Run(*p.Key); err != nil {
						continue
					}
				}

				result = append(result, *p.Key)
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return result, nil
}

// shouldSkip performs check, is we should skip files.
func (r *Reader) shouldSkip(path string, fileName *string) bool {
	return fileName == nil || ioStorage.IsDirectory(path, *fileName) && !r.WithNestedDir
}

// SetObjectsToStream set objects to stream.
func (r *Reader) SetObjectsToStream(list []string) {
	r.objectsToStream = list
}

// streamSetObjects streams preloaded objects.
func (r *Reader) streamSetObjects(ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error) {
	for i := range r.objectsToStream {
		r.openObject(ctx, r.objectsToStream[i], readersCh, errorsCh, true)
	}
}

// restoreObject restoring an archived object.
func (r *Reader) restoreObject(ctx context.Context, path string) error {
	days := int32(1) // Temporary restoration period (minimum 1 day)
	tier := types.TierExpedited

	_, err := r.client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: &r.bucketName,
		Key:    &path,
		RestoreRequest: &types.RestoreRequest{
			Days:        &days,
			Description: nil,
			GlacierJobParameters: &types.GlacierJobParameters{
				Tier: tier,
			},
			// TODO: check which tier we should use? Here or in GlacierJobParameters.Tier?
			Tier: types.TierExpedited,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to restore object: %w", err)
	}

	// Add to checking queue.
	r.objectsToWarm[path] = struct{}{}

	return nil
}

// checkObjectAvailability check if an object is available for download.
func (r *Reader) checkObjectAvailability(ctx context.Context, path string) (bool, error) {
	headOutput, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		return false, fmt.Errorf("failed to get head object: %w", err)
	}

	if headOutput.StorageClass != types.StorageClassGlacier &&
		headOutput.StorageClass != types.StorageClassDeepArchive &&
		headOutput.StorageClass != types.StorageClassGlacierIr {
		return true, nil
	}

	return false, nil
}

// warmStorage warms all directories in r.PathList.
func (r *Reader) warmStorage(ctx context.Context) error {
	for _, path := range r.PathList {
		if err := r.warmDirectory(ctx, path); err != nil {
			return fmt.Errorf("failed to warm directory %s: %w", path, err)
		}
	}

	r.Logger.Info("objects to restore", slog.Int("number", len(r.objectsToWarm)))

	// Start polling objects.
	if err := r.checkWarm(ctx); err != nil {
		return fmt.Errorf("failed to server directory warming: %w", err)
	}

	r.Logger.Info("storage warm up finished")

	return nil
}

// warmDirectory check files in directory, and if they need to be restored, restore them.
func (r *Reader) warmDirectory(ctx context.Context, path string) error {
	objects, err := r.ListObjects(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	for _, object := range objects {
		ok, err := r.checkObjectAvailability(ctx, object)
		if err != nil {
			return err
		}

		if !ok {
			if err = r.restoreObject(ctx, object); err != nil {
				return fmt.Errorf("failed to restore object: %w", err)
			}
		}
	}

	return nil
}

// checkWarm wait until all objects from r.objectsToWarm will be restored.
func (r *Reader) checkWarm(ctx context.Context) error {
	if len(r.objectsToWarm) == 0 {
		return nil
	}

	for path := range r.objectsToWarm {
		if err := r.pollWarmDirStatus(ctx, path); err != nil {
			return fmt.Errorf("failed to poll die status %s: %w", path, err)
		}
	}

	return nil
}

// pollWarmDirStatus polls the current status of directory that we are warming.
func (r *Reader) pollWarmDirStatus(ctx context.Context, path string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	r.Logger.Info("start polling status", slog.String("object", path))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ok, err := r.checkObjectAvailability(ctx, path)
			if err != nil {
				return err
			}

			r.Logger.Debug("object status",
				slog.String("object", path),
				slog.Bool("ok", ok),
			)

			if !ok {
				continue
			}

			// TODO: check if we should delete something from r.objectsToWarm

			return nil
		}
	}
}
