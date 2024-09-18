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
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

const bufferSize = 4096 * 1024 // 4mb

// Writer represents a local storage writer.
type Writer struct {
	// Optional parameters.
	options
	// Sync for running backup to one file.
	called atomic.Bool
}

// WithRemoveFiles adds remove files flag, so all files will be removed from backup folder before backup.
// Is used only for Writer.
func WithRemoveFiles() Opt {
	return func(r *options) {
		r.isRemovingFiles = true
	}
}

// NewWriter creates a new writer for local directory/file writes.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithRemoveFiles() - optional.
func NewWriter(ctx context.Context, opts ...Opt) (*Writer, error) {
	w := &Writer{}

	for _, opt := range opts {
		opt(&w.options)
	}

	if w.path == "" {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	var err error
	// If we want to remove files from backup path.
	if w.isRemovingFiles {
		err = w.RemoveFiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to remove files: %w", err)
		}
	}

	err = prepareBackupDirectory(w.path, w.isDir)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// prepareBackupDirectory creates the backup directory if it does not exist.
// It returns an error is the path already exits and it is not empty.
func prepareBackupDirectory(path string, isDir bool) error {
	if !isDir {
		path = filepath.Dir(path)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	return nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	info, err := os.Stat(w.path)

	switch {
	case err == nil:
		// ok.
	case os.IsNotExist(err):
		// File doesn't exist, it's ok.
		return nil
	default:
		return fmt.Errorf("failed to stat path %s: %w", w.path, err)
	}
	// if it is a file.
	if !info.IsDir() {
		if err = os.Remove(w.path); err != nil {
			return fmt.Errorf("failed to remove file %s: %w", w.path, err)
		}

		return nil
	}
	// If it is a dir.
	files, err := os.ReadDir(w.path)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", w.path, err)
	}

	for _, file := range files {
		filePath := filepath.Join(w.path, file.Name())
		if !file.IsDir() {
			if err = os.Remove(filePath); err != nil {
				return fmt.Errorf("failed to remove file %s: %w", filePath, err)
			}
		}
	}

	return nil
}

type bufferedFile struct {
	*bufio.Writer
	closer io.Closer
}

func (bf *bufferedFile) Close() error {
	err := bf.Writer.Flush()
	if err != nil {
		return err
	}

	return bf.closer.Close()
}

// NewWriter creates a new backup file in the given directory.
// The file name is based on the specified fileName.
func (w *Writer) NewWriter(ctx context.Context, fileName string) (io.WriteCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// protection for single file backup.
	if !w.isDir {
		if !w.called.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("parallel running for single file is not allowed")
		}
	}
	// We ignore `fileName` if `Writer` was initialized .WithFile()
	filePath := w.path
	if w.isDir {
		filePath = filepath.Join(w.path, fileName)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	return &bufferedFile{bufio.NewWriterSize(file, bufferSize), file}, nil
}

// GetType return `localType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return localType
}
