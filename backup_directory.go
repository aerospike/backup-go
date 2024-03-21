// Copyright 2024-2024 Aerospike, Inc.
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

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/internal/writers"
	"github.com/aerospike/backup-go/models"
)

// **** Backup To Directory Handler ****

type BackupToDirectoryStats struct {
	BackupStats
}

// BackupToDirectoryHandler handles a backup job to a directory
type BackupToDirectoryHandler struct {
	stats           BackupToDirectoryStats
	config          *BackupToDirectoryConfig
	aerospikeClient *a.Client
	errors          chan error
	directory       string
}

// newBackupToDirectoryHandler creates a new BackupToDirectoryHandler
func newBackupToDirectoryHandler(config *BackupToDirectoryConfig,
	ac *a.Client, directory string) *BackupToDirectoryHandler {
	return &BackupToDirectoryHandler{
		config:          config,
		aerospikeClient: ac,
		directory:       directory,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupToDirectoryHandler) run(ctx context.Context) {
	bwh.errors = make(chan error, 1)

	go func(errChan chan<- error) {
		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		err := prepareBackupDirectory(bwh.directory)
		if err != nil {
			errChan <- err
			return
		}

		writeWorkers := make([]*writeWorker[*models.Token], bwh.config.Parallel)

		var (
			fileID  atomic.Int32
			encoder encoding.Encoder
		)

		for i := range bwh.config.Parallel {
			encoder, err = bwh.config.EncoderFactory.CreateEncoder()
			if err != nil {
				errChan <- err
				return
			}

			writer, err := makeBackupFile(bwh.directory, bwh.config.Namespace, encoder, bwh.config.FileSizeLimit, &fileID)
			if err != nil {
				errChan <- err
				return
			}

			//nolint:gocritic // defer in loop is ok here,
			// we want to close the file after the backup is done
			defer writer.Close()

			dataWriter := newTokenWriter(encoder, writer)
			writeWorkers[i] = newWriteWorker(dataWriter)
		}

		handler := newBackupHandlerBase(&bwh.config.BackupConfig, bwh.aerospikeClient, bwh.config.Namespace)
		err = handler.run(ctx, writeWorkers)
		if err != nil {
			errChan <- err
			return
		}
	}(bwh.errors)
}

// GetStats returns the stats of the backup job
func (bwh *BackupToDirectoryHandler) GetStats() BackupToDirectoryStats {
	return bwh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bwh *BackupToDirectoryHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bwh.errors:
		return err
	}
}

// **** Helper Functions ****

// makeBackupFile creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The files is returned in write mode.
// If the fileSizeLimit is greater than 0, the file is wrapped in a Sized writer.
func makeBackupFile(dir, namespace string, encoder encoding.Encoder, fileSizeLimit uint64, fileID *atomic.Int32) (io.WriteCloser, error) {
	var (
		open func() (io.WriteCloser, error)
	)

	if _, ok := encoder.(*asb.Encoder); ok {
		open = func() (io.WriteCloser, error) {
			return getNewBackupFileASB(dir, namespace, int(fileID.Add(1)))
		}
	} else {
		open = func() (io.WriteCloser, error) {
			return getNewBackupFileGeneric(dir, namespace, int(fileID.Add(1)))
		}
	}

	writer, err := open()
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file: %w", err)
	}

	if fileSizeLimit > 0 {
		writer = writers.NewSized(fileSizeLimit, writer, open)
	}

	return writer, nil
}

var ErrBackupDirectoryInvalid = errors.New("backup directory is invalid")

func prepareBackupDirectory(dir string) error {
	DirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0o755)
			if err != nil {
				return fmt.Errorf("%w: failed to create backup directory %s: %v", ErrBackupDirectoryInvalid, dir, err)
			}
		}
	} else if !DirInfo.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrBackupDirectoryInvalid, dir)
	}

	fileInfo, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("%w: failed to read %s: %w", ErrBackupDirectoryInvalid, dir, err)
	}

	if len(fileInfo) > 0 {
		return fmt.Errorf("%w: %s is not empty", ErrBackupDirectoryInvalid, dir)
	}

	return nil
}

// getNewBackupFileGeneric creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The files is returned in write mode.
func getNewBackupFileGeneric(dir string, namespace string, id int) (*os.File, error) {
	fileName := getBackupFileNameGeneric(namespace, id)
	filePath := filepath.Join(dir, fileName)

	return openBackupFile(filePath)
}

// getNewBackupFileASB creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The files is returned in write mode.
// The file is created with an ASB header and .asb extension.
func getNewBackupFileASB(dir string, namespace string, id int) (*os.File, error) {
	fileName := getBackupFileNameASB(namespace, id)
	filePath := filepath.Join(dir, fileName)

	file, err := openBackupFile(filePath)
	if err != nil {
		return nil, err
	}

	_, err = writeASBHeader(file, namespace, id == 1)
	return file, err
}

func openBackupFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o666)
}

func getBackupFileNameGeneric(namespace string, id int) string {
	return fmt.Sprintf("%s_%d", namespace, id)
}

func getBackupFileNameASB(namespace string, id int) string {
	return getBackupFileNameGeneric(namespace, id) + ".asb"
}
