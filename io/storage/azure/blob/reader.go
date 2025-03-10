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

package blob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/models"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const azureBlobType = "azure-blob"

const (
	objStatusAvailable = iota
	objStatusArchived
	objStatusRestoring
)

// Reader represents GCP storage reader.
type Reader struct {
	// Optional parameters.
	ioStorage.Options

	client *azblob.Client

	// containerName contains name of the container to read from.
	containerName string

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string

	// objectsToWarm is used to track the current number of restoring objects.
	objectsToWarm []string
}

// NewReader returns new Azure blob directory/file reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(
	ctx context.Context,
	client *azblob.Client,
	containerName string,
	opts ...ioStorage.Opt,
) (*Reader, error) {
	r := &Reader{
		client: client,
	}

	// Set default val.
	r.PollWarmDuration = ioStorage.DefaultPollWarmDuration
	r.Logger = slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.Level(1024)}))

	for _, opt := range opts {
		opt(&r.Options)
	}

	if len(r.PathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	// Check if container exists.
	if _, err := client.ServiceClient().NewContainerClient(containerName).GetProperties(ctx, nil); err != nil {
		return nil, fmt.Errorf("unable to get container properties: %w", err)
	}

	r.containerName = containerName

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

	return r, nil
}

// StreamFiles streams file/directory form GCP cloud storage to `readersCh`.
// If error occurs, it will be sent to `errorsCh.`
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
			path = ioStorage.CleanPath(path, false)
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

func (r *Reader) streamDirectory(
	ctx context.Context, path string, readersCh chan<- models.File, errorsCh chan<- error,
) {
	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &path,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			errorsCh <- fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(path, *blobItem.Name) {
				continue
			}

			// Skip not valid files if validator is set.
			if r.Validator != nil {
				if err = r.Validator.Run(*blobItem.Name); err != nil {
					// Since we are passing invalid files, we don't need to handle this
					// error and write a test for it. Maybe we should log this information
					// for the user, so they know what is going on.
					continue
				}
			}

			r.openObject(ctx, *blobItem.Name, readersCh, errorsCh, true)
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

	resp, err := r.client.DownloadStream(ctx, r.containerName, path, nil)
	if err != nil {
		// Skip 404 not found error.
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound && skipNotFound {
			return
		}

		errorsCh <- fmt.Errorf("failed to open file %s: %w", path, err)

		return
	}

	if resp.Body != nil {
		readersCh <- models.File{Reader: resp.Body, Name: filepath.Base(path)}
	}
}

// StreamFile opens a single file from GCP cloud storage and sends io.Readers to the `readersCh`
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

// shouldSkip performs check, is we should skip files.
func (r *Reader) shouldSkip(path, fileName string) bool {
	return (ioStorage.IsDirectory(path, fileName) && !r.WithNestedDir) ||
		isSkippedByStartAfter(r.StartAfter, fileName)
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return azureBlobType
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context, path string) error {
	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &path,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(path, *blobItem.Name) {
				continue
			}

			switch {
			case r.Validator != nil:
				// If we found a valid file, return.
				if err = r.Validator.Run(*blobItem.Name); err == nil {
					return nil
				}
			default:
				// If we found anything, then folder is not empty.
				if blobItem.Name != nil {
					return nil
				}
			}
		}
	}

	return fmt.Errorf("%s is empty", path)
}

// ListObjects list all object in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	result := make([]string, 0)

	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &path,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(path, *blobItem.Name) {
				continue
			}

			if blobItem.Name != nil {
				if r.Validator != nil {
					if err = r.Validator.Run(*blobItem.Name); err != nil {
						continue
					}
				}

				result = append(result, *blobItem.Name)
			}
		}
	}

	return result, nil
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

func (r *Reader) rehydrateObject(ctx context.Context, path string, tier blob.AccessTier) error {
	r.Logger.Debug("starting rehydration", slog.String("path", path))
	bClient := r.client.ServiceClient().
		NewContainerClient(r.containerName).
		NewBlobClient(path)

	priority := blob.RehydratePriorityHigh

	_, err := bClient.SetTier(
		ctx,
		tier,
		&blob.SetTierOptions{
			RehydratePriority: &priority,
		})
	if err != nil {
		return fmt.Errorf("starting rehydration: %w", err)
	}

	return nil
}

// checkObjectAvailability check if an object is available for download.
func (r *Reader) checkObjectAvailability(ctx context.Context, path string) (int, error) {
	bClient := r.client.ServiceClient().
		NewContainerClient(r.containerName).
		NewBlobClient(path)

	objProps, err := bClient.GetProperties(ctx, nil)
	if err != nil {
		return objStatusArchived, fmt.Errorf("failed to get container properties: %w", err)
	}

	if objProps.AccessTier != nil && *objProps.AccessTier == string(blob.AccessTierArchive) {
		if objProps.ArchiveStatus != nil {
			// restoring
			return objStatusRestoring, nil
		}

		return objStatusArchived, nil
	}

	return objStatusAvailable, nil
}

// warmStorage warms all directories in r.PathList.
func (r *Reader) warmStorage(ctx context.Context, tier blob.AccessTier) error {
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

// warmDirectory warms files in directory, and if they need to be restored, restore them.
func (r *Reader) warmDirectory(ctx context.Context, path string, tier blob.AccessTier) error {
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
			if err = r.rehydrateObject(ctx, object, tier); err != nil {
				return fmt.Errorf("failed to restore object: %w", err)
			}

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
		r.Logger.Info("no objects to poll")

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

func parseAccessTier(tier string) (blob.AccessTier, error) {
	// To correct case: Tier
	tier = cases.Title(language.English).String(tier)

	var result blob.AccessTier

	possible := blob.PossibleAccessTierValues()

	for _, possibleTier := range possible {
		if tier == string(possibleTier) {
			result = possibleTier
			break
		}
	}

	switch result {
	case blob.AccessTierArchive:
		return "", fmt.Errorf("archive tier is not allowed")
	case "":
		return "", fmt.Errorf("invalid access tier %s", tier)
	default:
		return result, nil
	}
}

func isSkippedByStartAfter(startAfter, fileName string) bool {
	if startAfter == "" {
		return false
	}

	if fileName <= startAfter {
		return true
	}

	return false
}
