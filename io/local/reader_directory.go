package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/interfaces"
)

var ErrRestoreDirectoryInvalid = errors.New("restore directory is invalid")

type DirectoryStreamingReader struct {
	validator interfaces.Validator
	dir       string
}

var _ backup.StreamingReader = (*DirectoryStreamingReader)(nil)

func NewDirectoryStreamingReader(
	dir string,
	validator interfaces.Validator,
) (*DirectoryStreamingReader, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator cannot be nil")
	}

	return &DirectoryStreamingReader{
		dir:       dir,
		validator: validator,
	}, nil
}

// StreamFiles read files from disk and send io.Readers to `readersCh` communication chan for lazy loading.
// In case of error we send error to `errorsCh` channel.
func (f *DirectoryStreamingReader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	err := f.checkRestoreDirectory()
	if err != nil {
		errorsCh <- err
		return
	}

	fileInfo, err := os.ReadDir(f.dir)
	if err != nil {
		errorsCh <- fmt.Errorf("%w failed to read %s: %w", ErrRestoreDirectoryInvalid, f.dir, err)
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

		filePath := filepath.Join(f.dir, file.Name())
		if err = f.validator.Run(filePath); err != nil {
			// As we pass invalid files, we don't need process this error and write test for it.
			// Maybe we need to log this info, for user. So he will understand what happens.
			continue
		}

		var reader io.ReadCloser

		reader, err = os.Open(filePath)
		if err != nil {
			errorsCh <- fmt.Errorf("%w failed to open %s: %w", ErrRestoreDirectoryInvalid, filePath, err)
			return
		}

		readersCh <- reader
	}

	close(readersCh)
}

// checkRestoreDirectory checks that the restore directory exists,
// is a readable directory, and contains backup files of the correct format
func (f *DirectoryStreamingReader) checkRestoreDirectory() error {
	dir := f.dir

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

	return nil
}

func (f *DirectoryStreamingReader) GetType() string {
	return "directory"
}
