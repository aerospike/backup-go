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
	"strings"
)

const localType = "directory"

// Validator interface that describes backup files validator.
// Must be part of encoder implementation.
//
//go:generate mockery --name Validator
type validator interface {
	Run(fileName string) error
}

type StreamingReader struct {
	target
	validator validator
}

type target struct {
	path  string
	isDir bool
}

type Opts func(*target)

func WithDir(path string) Opts {
	return func(r *target) {
		r.path = path
		r.isDir = true
	}
}

func WithFile(path string) Opts {
	return func(r *target) {
		r.path = path
		r.isDir = false
	}
}

// NewDirectoryStreamingReader creates a new StreamingReader.
func NewDirectoryStreamingReader(
	validator validator,
	opts ...Opts,
) (*StreamingReader, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator cannot be nil")
	}

	if len(opts) < 1 {
		return nil, fmt.Errorf("at least one option is required")
	}

	r := &StreamingReader{
		validator: validator,
	}

	for _, opt := range opts {
		opt(&r.target)
	}

	return r, nil
}

// StreamFiles reads files from disk and sends io.Readers to the `readersCh`
// communication channel for lazy loading.
// In case of an error, it is sent to the `errorsCh` channel.
func (r *StreamingReader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	// If it is a folder, open and return.
	if r.isDir {
		r.openFolder(ctx, readersCh, errorsCh)
		return
	}

	// If not a folder, only file.
	r.openFile(ctx, r.path, readersCh, errorsCh)
}

func (r *StreamingReader) openFolder(
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
			continue
		}

		filePath := filepath.Join(r.path, file.Name())
		if err = r.validator.Run(filePath); err != nil {
			// Since we are passing invalid files, we don't need to handle this
			// error and write a test for it. Maybe we should log this information
			// for the user so they know what is going on.
			continue
		}

		var reader io.ReadCloser

		reader, err = os.Open(filePath)
		if err != nil {
			errorsCh <- fmt.Errorf("failed to open %s: %w", filePath, err)
			return
		}

		readersCh <- reader
	}

	close(readersCh)
}

// OpenFile opens single file and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *StreamingReader) openFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	defer close(readersCh)

	if ctx.Err() != nil {
		errorsCh <- ctx.Err()
		return
	}

	if !strings.Contains(filename, "/") {
		filename = fmt.Sprintf("%s/%s", r.path, filename)
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
func (r *StreamingReader) checkRestoreDirectory() error {
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
func (r *StreamingReader) GetType() string {
	return localType
}
