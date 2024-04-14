package backup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aerospike/backup-go/encoding"
)

func NewFileReaderFactory(dir string, decoder DecoderFactory) *FileReader {
	return &FileReader{dir: dir, decoder: decoder}
}

type FileReader struct {
	dir     string
	decoder DecoderFactory
}

func (f *FileReader) Readers() ([]io.ReadCloser, error) {
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
		filePath := filepath.Join(f.dir, file.Name())

		reader, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("%w failed to open %s: %w", ErrRestoreDirectoryInvalid, filePath, err)
		}

		readers = append(readers, reader)
	}

	return readers, nil
}

// **** Helper Functions ****

var ErrRestoreDirectoryInvalid = errors.New("restore directory is invalid")

// checkRestoreDirectory checks that the restore directory exists,
// is a readable directory, and contains backup files of the correct format
func (f *FileReader) checkRestoreDirectory() error {
	dir := f.dir
	decoding := f.decoder

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
