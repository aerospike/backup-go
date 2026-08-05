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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/io/storage/options"
)

const defaultBufferSize = 4096 * 1024 // 4mb

// Writer represents a local storage writer.
type Writer struct {
	// Optional parameters.
	options.Options
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

	if w.ChunkSize == 0 {
		w.ChunkSize = defaultBufferSize
	}

	// Special case for null target: writes are intentionally no-op.
	if filepath.Clean(w.PathList[0]) == filepath.Clean(os.DevNull) {
		return w, nil
	}

	if w.IsDir && !w.SkipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(w.PathList[0])
		if errors.Is(err, os.ErrNotExist) {
			// The directory is created lazily by NewWriter when the first
			// backup file is opened.
			return w, nil
		}

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

	// Create directly instead of checking with Stat first. A separate check
	// can become stale before MkdirAll runs if another process changes path.
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
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

// validateFilename ensures that generated files stay directly under the
// configured storage path. Local storage intentionally follows symlinks so it
// remains compatible with mounted and FUSE filesystems.
// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	return w.Remove(ctx, w.PathList[0])
}

// Remove deletes the file or directory contents specified by path.
func (w *Writer) Remove(ctx context.Context, targetPath string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if w.WithNestedDir {
		// RemoveAll is intentionally used directly. Stat-then-remove would
		// make the deletion decision on a stale path if it changes meanwhile.
		if err := os.RemoveAll(targetPath); err != nil {
			return fmt.Errorf("failed to remove targetPath %s: %w", targetPath, err)
		}

		return nil
	}

	root, err := os.OpenRoot(targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { // not exists, nothing to delete
			return nil
		}

		if isNotDir(err) { // it's a file, remove a single file
			if err = os.Remove(targetPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("failed to remove file %s: %w", targetPath, err)
			}

			return nil
		}

		return fmt.Errorf("failed to open %s: %w", targetPath, err)
	}

	defer root.Close()

	f, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("failed to open root directory %s: %w", targetPath, err)
	}
	defer f.Close()

	files, err := f.ReadDir(-1)
	if err != nil {
		return fmt.Errorf("failed to read root directory %s: %w", targetPath, err)
	}

	for _, file := range files {
		// Skip folders.
		if file.IsDir() {
			continue
		}
		// If validator is set, remove only valid files.
		if w.Validator != nil {
			// Pass the base filename to the validator, exactly as read from the directory.
			if err = w.Validator.Run(file.Name()); err != nil {
				continue
			}
		}

		if err = root.Remove(file.Name()); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return fmt.Errorf("failed to remove file %s: %w", file.Name(), err)
		}
	}

	return nil
}

// NewWriter creates a new backup file in the given directory.
// The file name is based on the specified fileName.
// isRecords specifies whether the file contains record data.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Special case for null target: writes are intentionally discarded.
	if filepath.Clean(w.PathList[0]) == filepath.Clean(os.DevNull) {
		return noopWriter{}, nil
	}

	// Create directory only if we have something to back up to this directory.
	err := createDirIfNotExist(w.PathList[0], w.IsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare backup directory: %w", err)
	}

	// We ignore `fileName` if `Writer` was initialized .WithFile()
	if err := common.ValidateFilename(filename); err != nil {
		return nil, err
	}

	switch {
	case w.IsDir:
		root, err := os.OpenRoot(w.PathList[0])
		if err != nil {
			return nil, fmt.Errorf("failed to open root %s: %w", w.PathList[0], err)
		}
		defer root.Close()

		file, err := root.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s in root %s: %w", filename, w.PathList[0], err)
		}

		return &bufferedFile{bufio.NewWriterSize(file, w.ChunkSize), file}, nil

	case !w.IsDir && filename != "":
		// If it is metadata file and we backup to one file.
		dir := filepath.Dir(w.PathList[0])

		root, err := os.OpenRoot(dir)
		if err != nil {
			return nil, fmt.Errorf("failed to open root %s: %w", dir, err)
		}
		defer root.Close()

		file, err := root.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s in root %s: %w", filename, dir, err)
		}

		return &bufferedFile{bufio.NewWriterSize(file, w.ChunkSize), file}, nil
	}

	// If we backup to one file (filename is empty).
	filePath := w.PathList[0]

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	return &bufferedFile{bufio.NewWriterSize(file, w.ChunkSize), file}, nil
}

// GetType returns the `localType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return TypeLocal
}

// GetOptions returns initialized options for the writer.
func (w *Writer) GetOptions() options.Options {
	return w.Options
}

func isNotDir(err error) bool {
	if err == nil {
		return false
	}
	// Check Unix syscall error
	if errors.Is(err, syscall.ENOTDIR) {
		return true
	}
	// Fallback check for unexported error strings
	return strings.Contains(err.Error(), "not a directory")
}
