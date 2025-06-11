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

package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/models"
)

const localType = "directory"

// Reader represents local storage reader.
type Reader struct {
	// Optional parameters.
	ioStorage.Options

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string

	// total size of all objects in a path.
	totalSize atomic.Int64
	// total number of objects in a path.
	totalNumber atomic.Int64
}

// NewReader creates a new local directory/file Reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(ctx context.Context, opts ...ioStorage.Opt) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.Options)
	}

	if len(r.PathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if r.IsDir {
		if !r.SkipDirCheck {
			for _, path := range r.PathList {
				if err := r.checkRestoreDirectory(path); err != nil {
					return nil, fmt.Errorf("%w: %w", ioStorage.ErrEmptyStorage, err)
				}
			}
		}

		if r.SortFiles && len(r.PathList) == 1 {
			if err := ioStorage.PreSort(ctx, r, r.PathList[0]); err != nil {
				return nil, fmt.Errorf("failed to pre sort: %w", err)
			}
		}
	}

	go r.calculateTotalSize()

	return r, nil
}

// StreamFiles reads file/directory from disk and sends io.Readers to the `readersCh`
// communication channel for lazy loading.
// In case of an error, it is sent to the `errorsCh` channel.
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
			if !r.SkipDirCheck {
				err := r.checkRestoreDirectory(path)
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
	fileInfo, err := os.ReadDir(path)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to read path %s: %w", path, err)
		return
	}

	for _, file := range fileInfo {
		if err = ctx.Err(); err != nil {
			errorsCh <- err
			return
		}

		if file.IsDir() {
			// Iterate over nested dirs recursively.
			if r.WithNestedDir {
				nestedDir := filepath.Join(path, file.Name())
				r.streamDirectory(ctx, nestedDir, readersCh, errorsCh)
			}

			continue
		}

		filePath := filepath.Join(path, file.Name())

		// Skip empty files.
		info, err := file.Info()
		if err != nil {
			errorsCh <- fmt.Errorf("failed to get file info %s: %w", filePath, err)
			return
		}

		if info.Size() == 0 {
			continue
		}

		if r.Validator != nil {
			if err = r.Validator.Run(filePath); err != nil {
				// Since we are passing invalid files, we don't need to handle this
				// error and write a test for it. Maybe we should log this information
				// for the user so they know what is going on.
				continue
			}
		}

		var reader io.ReadCloser

		reader, err = os.Open(filePath)
		if err != nil {
			errorsCh <- fmt.Errorf("failed to open %s: %w", filePath, err)
			return
		}

		readersCh <- models.File{Reader: reader, Name: filepath.Base(file.Name())}
	}
}

// StreamFile opens single file and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	if ctx.Err() != nil {
		errorsCh <- ctx.Err()
		return
	}

	reader, err := os.Open(filename)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to open %s: %w", filename, err)
		return
	}

	readersCh <- models.File{Reader: reader, Name: filepath.Base(filename)}
}

// checkRestoreDirectory checks that the restore directory exists,
// is a readable directory, and contains backup files of the correct format.
func (r *Reader) checkRestoreDirectory(dir string) error {
	dirInfo, err := os.Stat(dir)
	if err != nil {
		// Handle the error
		return fmt.Errorf("failed to get path info %s: %w", dir, err)
	}

	if !dirInfo.IsDir() {
		// Handle the case when it's not a directory
		return fmt.Errorf("%s is not a directory", dir)
	}

	fileInfo, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read path %s: %w", dir, err)
	}

	switch {
	case r.Validator != nil:
		for _, file := range fileInfo {
			if file.IsDir() {
				// Iterate over nested dirs recursively.
				if r.WithNestedDir {
					nestedDir := filepath.Join(dir, file.Name())
					// If the nested folder is ok, then return nil.
					if err = r.checkRestoreDirectory(nestedDir); err == nil {
						return nil
					}
				}

				continue
			}

			// If we found a valid file, return.
			if err = r.Validator.Run(file.Name()); err == nil {
				return nil
			}
		}

		return fmt.Errorf("%s is empty", dir)
	default:
		// Check if the directory is empty
		if len(fileInfo) == 0 {
			return fmt.Errorf("%s is empty", dir)
		}
	}

	return nil
}

// ListObjects list all object in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	result := make([]string, 0)

	fileInfo, err := os.ReadDir(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && r.SkipDirCheck {
			return nil, nil // Path doesn't exist, no error returned
		}

		return nil, fmt.Errorf("failed to read path %s: %w", path, err)
	}

	for i := range fileInfo {
		fullPath := filepath.Join(path, fileInfo[i].Name())

		if fileInfo[i].IsDir() {
			// If WithNestedDir is true, recursively list files in subdirectories
			if r.WithNestedDir {
				subDirFiles, err := r.ListObjects(ctx, fullPath)
				if err != nil {
					return nil, err
				}

				result = append(result, subDirFiles...)
			}

			continue
		}

		if r.Validator != nil {
			if err = r.Validator.Run(fileInfo[i].Name()); err != nil {
				continue
			}
		}

		result = append(result, fullPath)
	}

	return result, nil
}

// SetObjectsToStream sets objects to stream.
func (r *Reader) SetObjectsToStream(list []string) {
	r.objectsToStream = list
}

// streamSetObjects streams preloaded objects.
func (r *Reader) streamSetObjects(ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error) {
	for i := range r.objectsToStream {
		r.StreamFile(ctx, r.objectsToStream[i], readersCh, errorsCh)
	}
}

// GetType returns the type of the reader.
func (r *Reader) GetType() string {
	return localType
}

func (r *Reader) calculateTotalSize() {
	var (
		totalSize int64
		totalNum  int64
	)

	for _, path := range r.PathList {
		size, num, err := r.calculateTotalSizeForPath(path)
		if err != nil {
			r.Logger.Error("failed to calculate stats for path",
				slog.String("path", path),
				slog.Any("error", err),
			)
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

func (r *Reader) calculateTotalSizeForPath(path string) (totalSize, totalNum int64, err error) {
	dirInfo, err := os.Stat(path)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get path info %s: %w", path, err)
	}

	if !dirInfo.IsDir() {
		reader, err := os.Open(path)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to open %s: %w", path, err)
		}

		stat, err := reader.Stat()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get file stats %s: %w", path, err)
		}

		return stat.Size(), 1, nil
	}

	fileInfo, err := os.ReadDir(path)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read path %s: %w", path, err)
	}

	for _, file := range fileInfo {
		if file.IsDir() {
			// Iterate over nested dirs recursively.
			if r.WithNestedDir {
				nestedDir := filepath.Join(path, file.Name())
				// If the nested folder is ok, then return nil.
				if totalSize, totalNum, err = r.calculateTotalSizeForPath(nestedDir); err == nil {
					return totalSize, totalNum, nil
				}
			}

			continue
		}

		if r.Validator != nil {
			if err = r.Validator.Run(file.Name()); err == nil {
				info, err := file.Info()
				if err != nil {
					return 0, 0, fmt.Errorf("failed to get file info %s: %w", path, err)
				}

				totalNum++
				totalSize += info.Size()
			}
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
