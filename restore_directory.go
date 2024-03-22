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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
)

// **** Restore From Directory Handler ****

// RestoreFromDirectoryStats stores the status of a restore from directory job
type RestoreFromDirectoryStats struct {
	RestoreStats
}

// RestoreFromDirectoryHandler handles a restore job from a directory
type RestoreFromDirectoryHandler struct {
	stats           RestoreFromDirectoryStats
	config          *RestoreFromDirectoryConfig
	aerospikeClient *a.Client
	errors          chan error
	directory       string
}

// newRestoreFromDirectoryHandler creates a new RestoreFromDirectoryHandler
func newRestoreFromDirectoryHandler(config *RestoreFromDirectoryConfig,
	ac *a.Client, directory string) *RestoreFromDirectoryHandler {
	return &RestoreFromDirectoryHandler{
		config:          config,
		aerospikeClient: ac,
		directory:       directory,
	}
}

// run runs the restore job
// currently this should only be run once
func (rh *RestoreFromDirectoryHandler) run(ctx context.Context) {
	rh.errors = make(chan error, 1)

	go func(errChan chan<- error) {
		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		// Check that the restore directory is valid

		// open the directory
		// read the files rrh.config.Parallel at a time
		// create a buffered reader for each file
		// hand the readers to a restore handler and run it
		// wait for the restore handler to finish
		// if there are more files, continue to the next batch
		// if there are no more files, return

		err := checkRestoreDirectory(rh.directory, rh.config.DecoderFactory)
		if err != nil {
			errChan <- err
			return
		}

		fileInfo, err := os.ReadDir(rh.directory)
		if err != nil {
			errChan <- fmt.Errorf("%w failed to read %s: %w", ErrRestoreDirectoryInvalid, rh.directory, err)
			return
		}

		batchSize := rh.config.Parallel
		readers := []io.Reader{}

		for i, file := range fileInfo {
			filePath := filepath.Join(rh.directory, file.Name())

			reader, err := os.Open(filePath)
			if err != nil {
				err = fmt.Errorf("%w failed to open %s: %w", ErrRestoreDirectoryInvalid, filePath, err)
				errChan <- err

				return
			}

			//nolint:gocritic // defer in loop is ok here
			// we want to close the readers after the restore is done
			defer reader.Close()

			// buffer the reader to save memory
			readers = append(readers, bufio.NewReader(reader))

			// if we have not reached the batch size and we have more readers
			// continue to the next reader
			// if we are at the end of readers then run no matter what
			if i < len(fileInfo)-1 && len(readers) < batchSize {
				continue
			}

			restoreHandler := newRestoreHandler(&rh.config.RestoreConfig, rh.aerospikeClient, readers)
			restoreHandler.run(ctx)

			err = restoreHandler.Wait(ctx)
			if err != nil {
				errChan <- err
				return
			}

			readers = []io.Reader{}
		}
	}(rh.errors)
}

// GetStats returns the stats of the restore job
func (rh *RestoreFromDirectoryHandler) GetStats() RestoreFromDirectoryStats {
	return rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rh *RestoreFromDirectoryHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rh.errors:
		return err
	}
}

// **** Helper Functions ****

var ErrRestoreDirectoryInvalid = errors.New("restore directory is invalid")

// checkRestoreDirectory checks that the restore directory exists,
// is a readable directory, and contains backup files of the correct format
func checkRestoreDirectory(dir string, decoding DecoderFactory) error {
	dirInfo, err := os.Stat(dir)
	if err != nil {
		// Handle the error
		return fmt.Errorf("%w: failed to read %s: %w", ErrRestoreDirectoryInvalid, dir, err)
	}

	if !dirInfo.IsDir() {
		// Handle the case when it's not a directory
		return fmt.Errorf("%w: %s is not a directory", ErrRestoreDirectoryInvalid, dir)
	}

	fileInfo, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("%w: failed to read %s: %w", ErrRestoreDirectoryInvalid, dir, err)
	}

	// Check if the directory is empty
	if len(fileInfo) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrRestoreDirectoryInvalid, dir)
	}

	if err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("%w: failed reading restore file %s in %s: %v", ErrRestoreDirectoryInvalid, d.Name(), dir, err)
		}

		// this function gets called on the directory itself
		// we only want to check nested files so skip the root
		if d.Name() == filepath.Base(dir) {
			return nil
		}

		if d.IsDir() {
			return fmt.Errorf("%w: found directory %s in %s", ErrRestoreDirectoryInvalid, d.Name(), dir)
		}

		return verifyBackupFileExtension(d.Name(), decoding)
	}); err != nil {
		if errors.Is(err, ErrRestoreDirectoryInvalid) {
			return err
		}

		return fmt.Errorf("%w: failed to read %s: %v", ErrRestoreDirectoryInvalid, dir, err)
	}

	return nil
}

func verifyBackupFileExtension(fileName string, decoder DecoderFactory) error {
	if _, ok := decoder.(*encoding.ASBDecoderFactory); ok {
		if filepath.Ext(fileName) != ".asb" {
			return fmt.Errorf("%w, restore file %s is in an invalid format, expected extension: .asb, got: %s",
				ErrRestoreDirectoryInvalid, fileName, filepath.Ext(fileName))
		}
	}

	return nil
}
