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
	"sync"
)

const bufferSize = 4096 * 1024 // 4mb

type Writer struct {
	target

	once   sync.Once
	called bool
}

// NewDirectoryWriter creates a new writer for directory backups
//   - path is the target folder for backup.
//   - fileSizeLimit is the maximum size of each backup file in bytes.
//
// If FileSizeLimit is 0, backup file size is unbounded.
// If non-zero, backup files will be split into multiple files if their size
// exceeds this limit.
// If non-zero, FileSizeLimit must be greater than or equal to 1MB.
// FileSizeLimit is not a strict limit, the actual file size may exceed this
// limit by a small amount.
func NewDirectoryWriter(removeFiles bool, opts ...Opts,
) (*Writer, error) {
	if len(opts) < 1 {
		return nil, fmt.Errorf("at least one option is required")
	}

	w := &Writer{}

	for _, opt := range opts {
		opt(&w.target)
	}

	var err error

	if w.isDir {
		if removeFiles {
			err = forcePrepareBackupDirectory(w.path)
		} else {
			err = prepareBackupDirectory(w.path)
		}
	}

	if err != nil {
		return nil, err
	}

	return w, nil
}

// prepareBackupDirectory creates the backup directory if it does not exist.
// It returns an error is the path already exits and it is not empty.
func prepareBackupDirectory(dir string) error {
	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return makeDir(dir)
		}

		return err
	}

	if !dirInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	fileInfo, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read path %s: %w", dir, err)
	}

	if len(fileInfo) > 0 {
		return fmt.Errorf("%s is not empty", dir)
	}

	return nil
}

// forcePrepareBackupDirectory removes any existing directory and its contents
// and creates a new directory.
func forcePrepareBackupDirectory(dir string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return fmt.Errorf("failed to remove directory %s: %w", dir, err)
	}

	return makeDir(dir)
}

func makeDir(dir string) error {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return fmt.Errorf("failed to create backup directory %s: %w", dir, err)
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

	if w.called {
		return nil, fmt.Errorf("parallel running for one file is not allowed")
	}

	if !w.isDir {
		w.once.Do(func() {
			w.called = true
		})
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
