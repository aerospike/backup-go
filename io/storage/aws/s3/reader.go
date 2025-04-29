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
	"strings"
	"sync/atomic"
	"time"

	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsHttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	s3type               = "s3"
	restoreValueOngoing  = "ongoing-request=\"true\""
	restoreValueFinished = "ongoing-request=\"false\""
)

const (
	objStatusAvailable = iota
	objStatusArchived
	objStatusRestoring
)

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
	objectsToWarm []string

	// total size of all objects in a path.
	totalSize atomic.Int64
	// total number of objects in a path.
	totalNumber atomic.Int64
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

	// Set default val.
	r.PollWarmDuration = ioStorage.DefaultPollWarmDuration
	r.Logger = slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.Level(1024)}))

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

	if r.AccessTier != "" {
		r.Logger.Debug("start warming storage")

		r.objectsToWarm = make([]string, 0)

		tier, err := parseAccessTier(r.AccessTier)
		if err != nil {
			return nil, fmt.Errorf("failed to parse restore tier: %w", err)
		}

		r.Logger.Debug("parsed tier", slog.String("value", string(tier)))

		if err := r.warmStorage(ctx, tier); err != nil {
			return nil, fmt.Errorf("failed to heat the storage: %w", err)
		}

		r.Logger.Debug("finish warming storage")
	}
	// We "lazy" calculate total size of all files in a path for estimates calculations.
	go r.calculateTotalSize(ctx)

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
	state, err := r.checkObjectAvailability(ctx, path)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to check object availability: %w", err)
		return
	}

	if state != objStatusAvailable {
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
func (r *Reader) restoreObject(ctx context.Context, path string, tier types.Tier) error {
	days := int32(1) // Temporary restoration period (minimum 1 day)

	_, err := r.client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: &r.bucketName,
		Key:    &path,
		RestoreRequest: &types.RestoreRequest{
			Days: &days,
			GlacierJobParameters: &types.GlacierJobParameters{
				Tier: tier,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to restore object: %w", err)
	}

	return nil
}

// checkObjectAvailability check if an object is available for download.
func (r *Reader) checkObjectAvailability(ctx context.Context, path string) (int, error) {
	headOutput, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		return objStatusArchived, fmt.Errorf("failed to get head object %s %s: %w", r.bucketName, path, err)
	}

	r.Logger.Debug("check object availability",
		slog.Any("headOutput", headOutput),
	)

	if headOutput.Restore != nil {
		var exp string
		if headOutput.Expiration != nil {
			exp = *headOutput.Expiration
		}

		r.Logger.Debug("head out restore",
			slog.String("value", *headOutput.Restore),
			slog.String("expiration", exp),
		)

		switch {
		case strings.Contains(*headOutput.Restore, restoreValueOngoing):
			return objStatusRestoring, nil
		case strings.Contains(*headOutput.Restore, restoreValueFinished):
			// Means that object is already restored and will be accessible until headOutput.Expiration.
			return objStatusAvailable, nil
		default:
			return objStatusArchived, nil
		}
	}

	if headOutput.StorageClass == types.StorageClassGlacier ||
		headOutput.StorageClass == types.StorageClassDeepArchive ||
		headOutput.StorageClass == types.StorageClassGlacierIr {
		return objStatusArchived, nil
	}

	return objStatusAvailable, nil
}

// warmStorage warms all directories in r.PathList.
func (r *Reader) warmStorage(ctx context.Context, tier types.Tier) error {
	for _, path := range r.PathList {
		if err := r.warmDirectory(ctx, path, tier); err != nil {
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
func (r *Reader) warmDirectory(ctx context.Context, path string, tier types.Tier) error {
	objects, err := r.ListObjects(ctx, path)
	if err != nil {
		return err
	}

	for _, object := range objects {
		state, err := r.checkObjectAvailability(ctx, object)
		if err != nil {
			return err
		}

		switch state {
		case objStatusArchived:
			if err = r.restoreObject(ctx, object, tier); err != nil {
				return fmt.Errorf("failed to restore object: %w", err)
			}
			// Add to checking queue.
			r.objectsToWarm = append(r.objectsToWarm, object)
		case objStatusRestoring:
			// Add for checking status.
			r.objectsToWarm = append(r.objectsToWarm, object)
		default: // ok.
		}
	}

	return nil
}

// checkWarm wait until all objects from r.objectsToWarm will be restored.
func (r *Reader) checkWarm(ctx context.Context) error {
	if len(r.objectsToWarm) == 0 {
		return nil
	}

	for i := range r.objectsToWarm {
		if err := r.pollWarmDirStatus(ctx, r.objectsToWarm[i]); err != nil {
			return fmt.Errorf("failed to poll dir status %s: %w", r.objectsToWarm[i], err)
		}
	}

	return nil
}

// pollWarmDirStatus polls the current status of directory that we are warming.
func (r *Reader) pollWarmDirStatus(ctx context.Context, path string) error {
	ticker := time.NewTicker(r.PollWarmDuration)
	defer ticker.Stop()

	r.Logger.Info("start polling status", slog.String("object", path))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			state, err := r.checkObjectAvailability(ctx, path)
			if err != nil {
				return err
			}

			r.Logger.Debug("object status",
				slog.String("object", path),
				slog.Int("state", state),
			)

			if state != objStatusAvailable {
				continue
			}

			return nil
		}
	}
}

func parseAccessTier(tier string) (types.Tier, error) {
	// To correct case: Tier
	tier = cases.Title(language.English).String(tier)

	var result types.Tier
	possible := result.Values()

	for _, possibleTier := range possible {
		if tier == string(possibleTier) {
			result = possibleTier
			break
		}
	}

	if result == "" {
		return "", fmt.Errorf("invalid access tier %s", tier)
	}

	return result, nil
}

func (r *Reader) calculateTotalSize(ctx context.Context) {
	var (
		totalSize int64
		totalNum  int64
	)

	for _, path := range r.PathList {
		size, num, err := r.calculateTotalSizeForPath(ctx, path)
		if err != nil {
			// Skip calculation errors.
			return
		}

		totalSize += size
		totalNum += num
	}

	// set size when everything is ready.
	r.totalSize.Store(totalSize)
	r.totalNumber.Store(totalNum)
}

func (r *Reader) calculateTotalSizeForPath(ctx context.Context, path string) (totalSize, totalNum int64, err error) {
	// if we have file to calculate.
	if !r.IsDir {
		headOutput, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(r.bucketName),
			Key:    aws.String(path),
		})
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get head object %s %s: %w", r.bucketName, path, err)
		}

		return *headOutput.ContentLength, 1, nil
	}

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
			return 0, 0, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
				continue
			}

			if r.Validator != nil {
				if err = r.Validator.Run(*p.Key); err == nil {
					totalNum++
					totalSize += *p.Size
				}
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return totalSize, totalNum, nil
}

// GetSize returns the size of asb/asbx file/dir that was initialized.
func (r *Reader) GetSize() int64 {
	return r.totalSize.Load()
}

// GetNumber returns the number of asb/asbx files/dirs that was initialized.
func (r *Reader) GetNumber() int64 {
	return r.totalNumber.Load()
}
