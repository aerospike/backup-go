package local

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
)

func NewDirectoryReaderFactory(dir string, decoder encoding.DecoderFactory) (*DirectoryReaderFactory, error) {
	if decoder == nil {
		return nil, errors.New("decoder is nil")
	}

	return &DirectoryReaderFactory{dir: dir, decoder: decoder}, nil
}

var _ backup.ReaderFactory = (*DirectoryReaderFactory)(nil)

var ErrRestoreDirectoryInvalid = errors.New("restore directory is invalid")

type DirectoryReaderFactory struct {
	decoder encoding.DecoderFactory
	dir     string
}

func (f *DirectoryReaderFactory) Readers() ([]io.ReadCloser, error) {
	err := f.checkRestoreDirectory()
	if err != nil {
		return nil, err
	}

	fileInfo, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("%w failed to read %s: %w", ErrRestoreDirectoryInvalid, f.dir, err)
	}

	readers := make([]io.ReadCloser, 0, len(fileInfo))

	for _, file := range fileInfo {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(f.dir, file.Name())
		if err := f.decoder.Validate(filePath); err != nil {
			continue
		}

		reader, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("%w failed to open %s: %w", ErrRestoreDirectoryInvalid, filePath, err)
		}

		readers = append(readers, reader)
	}

	if len(readers) == 0 {
		return nil, fmt.Errorf("%w: %s doesn't contain backup files", ErrRestoreDirectoryInvalid, f.dir)
	}

	return readers, nil
}

// checkRestoreDirectory checks that the restore directory exists,
// is a readable directory, and contains backup files of the correct format
func (f *DirectoryReaderFactory) checkRestoreDirectory() error {
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

func (f *DirectoryReaderFactory) GetType() string {
	return "directory"
}
