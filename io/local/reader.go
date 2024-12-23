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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const localType = "directory"

// Validator interface that describes backup files validator.
// Must be part of encoder implementation.
//
//go:generate mockery --name Validator
type validator interface {
	Run(fileName string) error
}

// Reader represents local storage reader.
type Reader struct {
	// Optional parameters.
	options
}

// NewReader creates a new local directory/file Reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(opts ...Opt) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.options)
	}

	if len(r.pathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if r.sort != "" && r.sort != SortASC && r.sort != SortDESC {
		return nil, fmt.Errorf("unknown sorting type %s", r.sort)
	}

	return r, nil
}

// StreamFiles reads file/directory from disk and sends io.Readers to the `readersCh`
// communication channel for lazy loading.
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	defer close(readersCh)

	for _, path := range r.pathList {
		// If it is a folder, open and return.
		switch r.isDir {
		case true:
			err := r.checkRestoreDirectory(path)
			if err != nil {
				errorsCh <- err
				return
			}

			r.streamDirectory(ctx, path, readersCh, errorsCh)
		case false:
			// If not a folder, only file.
			r.StreamFile(ctx, path, readersCh, errorsCh)
		}
	}
}

func (r *Reader) streamDirectory(
	ctx context.Context, path string, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	fileInfo, err := r.getFilesList(path)
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
			if r.withNestedDir {
				nestedDir := filepath.Join(path, file.Name())
				r.streamDirectory(ctx, nestedDir, readersCh, errorsCh)
			}

			continue
		}

		filePath := filepath.Join(path, file.Name())

		if r.validator != nil {
			if err = r.validator.Run(filePath); err != nil {
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

		readersCh <- reader
	}
}

// StreamFile opens single file and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	if ctx.Err() != nil {
		errorsCh <- ctx.Err()
		return
	}

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

	reader, err := os.Open(filename)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to open %s: %w", filename, err)
		return
	}

	readersCh <- reader
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
	case r.validator != nil:
		for _, file := range fileInfo {
			if file.IsDir() {
				// Iterate over nested dirs recursively.
				if r.withNestedDir {
					nestedDir := filepath.Join(dir, file.Name())
					// If the nested folder is ok, then return nil.
					if err = r.checkRestoreDirectory(nestedDir); err == nil {
						return nil
					}
				}

				continue
			}

			// If we found a valid file, return.
			if err = r.validator.Run(file.Name()); err == nil {
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

// getFilesList returns sorted or unsorted files list from directory.
func (r *Reader) getFilesList(path string) ([]os.DirEntry, error) {
	fileInfo, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read path %s: %w", path, err)
	}

	switch r.sort {
	case "":
		return fileInfo, nil
	case SortASC:
		sort.Slice(fileInfo, func(i, j int) bool {
			return fileInfo[i].Name() < fileInfo[j].Name()
		})
	case SortDESC:
		sort.Slice(fileInfo, func(i, j int) bool {
			return fileInfo[i].Name() > fileInfo[j].Name()
		})
	}

	return fileInfo, nil
}

// GetType returns the type of the reader.
func (r *Reader) GetType() string {
	return localType
}
