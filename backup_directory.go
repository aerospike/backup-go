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
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/writers"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// **** Backup To Directory Handler ****

type BackupToDirectoryStats struct {
	BackupStats
}

// BackupToDirectoryHandler handles a backup job to a directory
type BackupToDirectoryHandler struct {
	config          *BackupToDirectoryConfig
	aerospikeClient *a.Client
	errors          chan error
	logger          *slog.Logger
	directory       string
	id              string
	stats           BackupToDirectoryStats
}

// newBackupToDirectoryHandler creates a new BackupToDirectoryHandler
func newBackupToDirectoryHandler(config *BackupToDirectoryConfig,
	ac *a.Client, directory string, logger *slog.Logger) *BackupToDirectoryHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackupDirectory)
	logger.Debug("created new backup to directory handler", "directory", directory)

	return &BackupToDirectoryHandler{
		config:          config,
		aerospikeClient: ac,
		directory:       directory,
		id:              id,
		logger:          logger,
	}
}

// run runs the backup job
// currently this should only be run once
func (bh *BackupToDirectoryHandler) run(ctx context.Context) {
	bh.errors = make(chan error, 1)

	go doWork(bh.errors, bh.logger, func() error {
		bh.logger.Debug("preparing backup directory", "directory", bh.directory)

		err := prepareBackupDirectory(bh.directory)
		if err != nil {
			return err
		}

		writeWorkers := make([]*writeWorker[*models.Token], bh.config.Parallel)

		var (
			// If we are using a file size limit,
			// the writers will open new files as they hit the limit.
			// Writers may be running in multiple threads, so we need to
			// use atomics to keep track of the current file id.
			fileID  atomic.Int32
			encoder encoding.Encoder
		)

		for i := range bh.config.Parallel {
			encoder, err = bh.config.EncoderFactory.CreateEncoder()
			if err != nil {
				return err
			}

			writer, err := makeBackupFile(bh.directory, bh.config.Namespace, encoder, bh.config.FileSizeLimit, &fileID)
			if err != nil {
				return err
			}

			//nolint:gocritic // defer in loop is ok here,
			// we want to close the file after the backup is done
			defer func() {
				if err := writer.Close(); err != nil {
					bh.logger.Error("failed to close backup file", "error", err)
				}
			}()

			// backup secondary indexes and UDFs on the first writer
			// this is done to match the behavior of the
			// backup c tool and keep the backup files more consistent
			// at some point we may want to treat the secondary indexes/UDFs
			// like records and back them up as part of the same pipeline
			// but doing so would cause them to be mixed in with records in the backup file(s)
			if i == 0 {
				err = backupSIndexes(ctx, bh.aerospikeClient, &bh.config.BackupConfig, writer, bh.logger)
				if err != nil {
					return err
				}

				err = backupUDFs(ctx, bh.aerospikeClient, &bh.config.BackupConfig, writer, bh.logger)
				if err != nil {
					return err
				}
			}

			var dataWriter dataWriter[*models.Token] = newTokenWriter(encoder, writer, bh.logger)
			dataWriter = newWriterWithTokenStats(dataWriter, &bh.stats.BackupStats, bh.logger)
			writeWorkers[i] = newWriteWorker(dataWriter)
		}

		handler := newBackupHandlerBase(&bh.config.BackupConfig, bh.aerospikeClient, bh.config.Namespace, bh.logger)

		return handler.run(ctx, writeWorkers)
	})
}

// GetStats returns the stats of the backup job
func (bh *BackupToDirectoryHandler) GetStats() *BackupToDirectoryStats {
	return &bh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bh *BackupToDirectoryHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bh.errors:
		return err
	}
}

// **** Helper Functions ****

// makeBackupFile creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The file is returned in write mode.
// If the fileSizeLimit is greater than 0, the file is wrapped in a Sized writer.
func makeBackupFile(dir, namespace string, encoder encoding.Encoder,
	fileSizeLimit int64, fileID *atomic.Int32) (io.WriteCloser, error) {
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
func getNewBackupFileGeneric(dir, namespace string, id int) (*os.File, error) {
	fileName := getBackupFileNameGeneric(namespace, id)
	filePath := filepath.Join(dir, fileName)

	return openBackupFile(filePath)
}

// getNewBackupFileASB creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The files is returned in write mode.
// The file is created with an ASB header and .asb extension.
func getNewBackupFileASB(dir, namespace string, id int) (*os.File, error) {
	fileName := getBackupFileNameASB(namespace, id)
	filePath := filepath.Join(dir, fileName)

	file, err := openBackupFile(filePath)
	if err != nil {
		return nil, err
	}

	return file, writeASBHeader(file, namespace, id == 1)
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
