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

type options struct {
	// path contains path to file or directory.
	path string
	// isDir flag describes what we have in path, file or directory.
	isDir bool
	// isRemovingFiles flag describes should we remove everything from backup folder or not.
	isRemovingFiles bool
	// validator contains files validator that is applied to files if isDir = true.
	validator validator
	// withNestedDir describes if we should check for if an object is a directory for read/write operations.
	// When we stream files or delete files in folder, we skip directories. This flag will avoid skipping.
	// Default: true
	withNestedDir bool
}

type Opt func(*options)

// WithDir adds directory to reading/writing files from/to.
func WithDir(path string) Opt {
	return func(r *options) {
		r.path = path
		r.isDir = true
	}
}

// WithFile adds a file path to reading/writing from/to.
func WithFile(path string) Opt {
	return func(r *options) {
		r.path = path
		r.isDir = false
	}
}

// WithValidator adds validator to Reader, so files will be validated before reading.
// Is used only for Reader.
func WithValidator(v validator) Opt {
	return func(r *options) {
		r.validator = v
	}
}

// WithNestedDir adds withNestedDir = true parameter. That means that we won't skip nested folders.
func WithNestedDir() Opt {
	return func(r *options) {
		r.withNestedDir = true
	}
}

// NewReader creates a new local directory/file Reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(opts ...Opt) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.options)
	}

	if r.path == "" {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
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
	// If it is a folder, open and return.
	if r.isDir {
		r.streamDirectory(ctx, readersCh, errorsCh)
		return
	}

	// If not a folder, only file.
	r.streamFile(ctx, r.path, readersCh, errorsCh)
}

func (r *Reader) streamDirectory(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	err := r.checkRestoreDirectory()
	if err != nil {
		errorsCh <- err
		return
	}

	fileInfo, err := os.ReadDir(r.path)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to read path %s: %w", r.path, err)
		return
	}

	for _, file := range fileInfo {
		if err = ctx.Err(); err != nil {
			errorsCh <- err
			return
		}

		if file.IsDir() {
			// Itterate over nested dirs recursively.
			if r.withNestedDir {
				nestedDir := filepath.Join(r.path, file.Name())

				subReader, err := NewReader(WithDir(nestedDir), WithValidator(r.validator), WithNestedDir())
				if err != nil {
					errorsCh <- fmt.Errorf("failed to read nested dir %s: %w", nestedDir, err)
				}

				subReader.streamDirectory(ctx, readersCh, errorsCh)
			}

			continue
		}

		filePath := filepath.Join(r.path, file.Name())

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

// streamFile opens single file and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) streamFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	if ctx.Err() != nil {
		errorsCh <- ctx.Err()
		return
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
func (r *Reader) checkRestoreDirectory() error {
	dir := r.path

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

	// Check if the directory is empty
	if len(fileInfo) == 0 {
		return fmt.Errorf("%s is empty", dir)
	}

	return nil
}

// GetType returns the type of the reader.
func (r *Reader) GetType() string {
	return localType
}
