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

	"github.com/aerospike/backup-go/io/storage/options"
)

const bufferSize = 4096 * 1024 // 4mb

// Writer represents a local storage writer.
type Writer struct {
	// Optional parameters.
	options.Options
	// Sync for running backup to one file.
	called atomic.Bool
}

// NewWriter creates a new writer for local directory/file writes.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithRemoveFiles() - optional.
func NewWriter(ctx context.Context, opts ...options.Opt) (*Writer, error) {
	w := &Writer{}

	for _, opt := range opts {
		opt(&w.Options)
	}

	if len(w.PathList) != 1 {
		return nil, fmt.Errorf("one path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	// If directory not exists, we don't need to check it for emptiness.
	// For writer wi don't support pathList, so our path will always be the first element.
	path := w.PathList[0]
	if !w.IsDir {
		path = filepath.Dir(w.PathList[0])
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return w, nil
	}

	if w.IsDir && !w.SkipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(w.PathList[0])
		if err != nil {
			return nil, fmt.Errorf("failed to check if directory is empty: %w", err)
		}

		if !isEmpty && !w.IsRemovingFiles {
			return nil, fmt.Errorf("backup folder must be empty or set RemoveFiles = true")
		}
	}

	// If we want to remove files from backup path.
	if w.IsRemovingFiles {
		err := w.RemoveFiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to remove files: %w", err)
		}
	}

	return w, nil
}

// createDirIfNotExist creates the backup directory if it does not exist.
// It returns an error is the path already exits and it is not empty.
func createDirIfNotExist(path string, isDir bool) error {
	if !isDir {
		path = filepath.Dir(path)
	}

	_, err := os.Stat(path)

	switch {
	case err == nil:
		// ok.
	case os.IsNotExist(err):
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	default:
		return fmt.Errorf("failed to get stats for directory %s: %w", path, err)
	}

	return nil
}

func isEmptyDirectory(path string) (bool, error) {
	fileInfo, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("failed to read path %s: %w", path, err)
	}

	if len(fileInfo) > 0 {
		return false, nil
	}

	return true, nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	return w.Remove(ctx, w.PathList[0])
}

// Remove deletes the file or directory contents specified by path.
func (w *Writer) Remove(ctx context.Context, targetPath string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	info, err := os.Stat(targetPath)

	switch {
	case err == nil:
		// ok.
	case os.IsNotExist(err):
		// File doesn't exist, it's ok.
		return nil
	default:
		return fmt.Errorf("failed to stat targetPath %s: %w", targetPath, err)
	}
	// if it is a file.
	if !info.IsDir() {
		if err = os.Remove(targetPath); err != nil {
			return fmt.Errorf("failed to remove file %s: %w", targetPath, err)
		}

		return nil
	}

	if w.WithNestedDir {
		if err = os.RemoveAll(targetPath); err != nil {
			return fmt.Errorf("failed to remove targetPath %s: %w", targetPath, err)
		}

		return nil
	}

	// If it is a dir.
	files, err := os.ReadDir(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", targetPath, err)
	}

	for _, file := range files {
		filePath := filepath.Join(targetPath, file.Name())
		// Skip folders.
		if file.IsDir() {
			continue
		}
		// If validator is set, remove only valid files.
		if w.Validator != nil {
			if err = w.Validator.Run(filePath); err != nil {
				continue
			}
		}

		if err = os.Remove(filePath); err != nil {
			return fmt.Errorf("failed to remove file %s: %w", filePath, err)
		}
	}

	return nil
}

type bufferedFile struct {
	*bufio.Writer
	closer io.Closer
}

func (bf *bufferedFile) Close() error {
	err := bf.Flush()
	if err != nil {
		return err
	}

	return bf.closer.Close()
}

// NewWriter creates a new backup file in the given directory.
// The file name is based on the specified fileName.
// isMeta describe if the file is a metadata file.
func (w *Writer) NewWriter(ctx context.Context, fileName string, isMeta bool) (io.WriteCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Create directory only if we have something to back up to this directory.
	err := createDirIfNotExist(w.PathList[0], w.IsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare backup directory: %w", err)
	}

	// protection for single file backup.
	if !w.IsDir {
		if !isMeta && !w.called.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("parallel running for single file is not allowed")
		}
	}
	// We ignore `fileName` if `Writer` was initialized .WithFile()
	var filePath string
	switch {
	case w.IsDir:
		// If it is directory.
		filePath = filepath.Join(w.PathList[0], fileName)
	case isMeta && !w.IsDir:
		// If it is metadata file and we backup to one file.
		filePath = filepath.Join(filepath.Dir(w.PathList[0]), fileName)
	default:
		// If we backup to one file.
		filePath = w.PathList[0]
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	return &bufferedFile{bufio.NewWriterSize(file, bufferSize), file}, nil
}

// GetType returns the `localType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return localType
}
