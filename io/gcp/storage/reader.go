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

package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/aerospike/backup-go/models"
	"google.golang.org/api/iterator"
)

const (
	gcpStorageType = "gcp-storage"
	bufferSize     = 256
)

type validator interface {
	Run(fileName string) error
}

// Reader represents GCP storage reader.
type Reader struct {
	// Optional parameters.
	options

	// bucketHandle contains storage bucket handler for performing reading and writing operations.
	bucketHandle *storage.BucketHandle

	// bucketName contains name of the bucket to read from.
	bucketName string

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string
}

// NewReader returns new GCP storage directory/file reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
	opts ...Opt,
) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.options)
	}

	if len(r.pathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if r.sort != "" && r.sort != SortAsc && r.sort != SortDesc {
		return nil, fmt.Errorf("unknown sorting type %s", r.sort)
	}

	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid errors.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket attr:%s:  %v", bucketName, err)
	}

	r.bucketHandle = bucket
	r.bucketName = bucketName

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

	for _, path := range r.pathList {
		// If it is a folder, open and return.
		switch r.isDir {
		case true:
			path = cleanPath(path)
			if !r.skipDirCheck {
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
	// start serving goroutines.
	var wg sync.WaitGroup

	wg.Add(1)

	objectsToProcess := make(chan *string, bufferSize)

	go func() {
		defer wg.Done()
		r.processObjects(ctx, objectsToProcess, readersCh, errorsCh)
	}()

	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.startOffset,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				errorsCh <- fmt.Errorf("failed to read object attr from bucket %s: %w",
					r.bucketName, err)
			}
			// If the previous call to Next returned an error other than iterator.Done, all
			// subsequent calls will return the same error. To continue iteration, a new
			// `ObjectIterator` must be created.
			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs.Name) {
			continue
		}

		// Skip not valid files if validator is set.
		if r.validator != nil {
			if err = r.validator.Run(objAttrs.Name); err != nil {
				// Since we are passing invalid files, we don't need to handle this
				// error and write a test for it. Maybe we should log this information
				// for the user so they know what is going on.
				continue
			}
		}

		objectsToProcess <- &objAttrs.Name
	}

	close(objectsToProcess)
	wg.Wait()
}

// processObjects receives keys, sort them, and then send to open chan.
func (r *Reader) processObjects(
	ctx context.Context,
	objectsToProcess <-chan *string,
	readersCh chan<- models.File,
	errorsCh chan<- error,
) {
	// If we don't need to sort objects, open them.
	if r.sort == "" {
		for path := range objectsToProcess {
			r.openObject(ctx, path, readersCh, errorsCh)
		}

		return
	}
	// If we need to sort.
	keys := make([]string, 0)

	for key := range objectsToProcess {
		if ctx.Err() != nil {
			return
		}

		keys = append(keys, *key)
	}

	switch r.sort {
	case SortAsc:
		sort.Strings(keys)
	case SortDesc:
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	default:
		return
	}

	for _, k := range keys {
		r.openObject(ctx, &k, readersCh, errorsCh)
	}
}

// openObject creates object readers and sends them readersCh.
func (r *Reader) openObject(
	ctx context.Context,
	path *string,
	readersCh chan<- models.File,
	errorsCh chan<- error,
) {
	reader, err := r.bucketHandle.Object(*path).NewReader(ctx)
	if err != nil {
		// Skip 404 not found error.
		if errors.Is(err, storage.ErrObjectNotExist) {
			return
		}
		errorsCh <- fmt.Errorf("failed to open directory file %s: %w", *path, err)

		return
	}

	if reader != nil {
		readersCh <- models.File{Reader: reader, Name: filepath.Base(*path)}
	}
}

// StreamFile opens a single file from GCP cloud storage and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	// This condition will be true, only if we initialized reader for directory and then want to read
	// a specific file. It is used for state file and by asb service. So it must be initialized with only
	// one path.
	if r.isDir {
		if len(r.pathList) != 1 {
			errorsCh <- fmt.Errorf("reader must be initialized with only one path")
			return
		}

		filename = filepath.Join(r.pathList[0], filename)
	}

	reader, err := r.bucketHandle.Object(filename).NewReader(ctx)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to open file %s: %w", filename, err)
		return
	}

	if reader != nil {
		readersCh <- models.File{Reader: reader, Name: filepath.Base(filename)}
	}
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return gcpStorageType
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context, path string) error {
	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.startOffset,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				return fmt.Errorf("failed to read object attr from bucket %s: %w",
					r.bucketName, err)
			}
			// If the previous call to Next returned an error other than iterator.Done, all
			// subsequent calls will return the same error. To continue iteration, a new
			// `ObjectIterator` must be created.
			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs.Name) {
			continue
		}

		switch {
		case r.validator != nil:
			// If we found a valid file, return.
			if err = r.validator.Run(objAttrs.Name); err == nil {
				return nil
			}
		default:
			// If we found anything, then folder is not empty.
			if objAttrs.Name != "" {
				return nil
			}
		}
	}

	return fmt.Errorf("%s is empty", path)
}

// ListObjects list all object in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	result := make([]string, 0)

	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.startOffset,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				return nil, fmt.Errorf("failed to read object attr from bucket %s: %w",
					r.bucketName, err)
			}

			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs.Name) {
			continue
		}

		if objAttrs.Name != "" {
			result = append(result, objAttrs.Name)
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
	objectsToProcess := make(chan *string, bufferSize)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		r.processObjects(ctx, objectsToProcess, readersCh, errorsCh)
	}()

	for i := range r.objectsToStream {
		k := r.objectsToStream[i]
		objectsToProcess <- &k
	}

	close(objectsToProcess)
	wg.Wait()
}

// shouldSkip performs check, is we should skip files.
func (r *Reader) shouldSkip(path, fileName string) bool {
	return isDirectory(path, fileName) && !r.withNestedDir
}

func isDirectory(prefix, fileName string) bool {
	// If file name ends with / it is 100% dir.
	if strings.HasSuffix(fileName, "/") {
		return true
	}

	// If we look inside some folder.
	if strings.HasPrefix(fileName, prefix) {
		// For root folder we should add.
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		clean := strings.TrimPrefix(fileName, prefix)

		return strings.Contains(clean, "/")
	}
	// All other variants.
	return strings.Contains(fileName, "/")
}

// cleanPath is protection from incorrect input.
func cleanPath(path string) string {
	result := path
	// Protection from incorrect input.
	if !strings.HasSuffix(path, "/") && path != "/" && path != "" {
		result = fmt.Sprintf("%s/", path)
	}

	return result
}
