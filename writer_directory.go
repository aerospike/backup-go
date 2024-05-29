package backup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/writers"
)

type DirectoryWriterFactory struct {
	fileID    *atomic.Uint32
	encoder   encoding.EncoderFactory
	directory string

	fileSizeLimit int64
}

var _ WriteFactory = (*DirectoryWriterFactory)(nil)

// NewDirectoryWriterFactory creates new factory for directory backups
// dir is target folder for backup
// fileSizeLimit is the maximum size of each backup file in bytes.
// If FileSizeLimit is 0, backup file size is unbounded.
// If non-zero, backup files will be split into multiple files if their size exceeds this limit.
// If non-zero, FileSizeLimit must be greater than or equal to 1MB.
// FileSizeLimit is not a strict limit, the actual file size may exceed this limit by a small amount.
func NewDirectoryWriterFactory(dir string, fileSizeLimit int64, encoder encoding.EncoderFactory, removeFiles bool,
) (*DirectoryWriterFactory, error) {
	if fileSizeLimit > 0 && fileSizeLimit < 1024*1024 {
		return nil, fmt.Errorf("file size limit must be 0 for no limit, or at least 1MB, got %d", fileSizeLimit)
	}

	if fileSizeLimit < 0 {
		return nil, fmt.Errorf("file size limit must not be negative, got %d", fileSizeLimit)
	}

	var err error
	if removeFiles {
		err = forcePrepareBackupDirectory(dir)
	} else {
		err = prepareBackupDirectory(dir)
	}

	if err != nil {
		return nil, err
	}

	if encoder == nil {
		encoder = defaultEncoderFactory
	}

	return &DirectoryWriterFactory{
		directory:     dir,
		fileID:        &atomic.Uint32{},
		fileSizeLimit: fileSizeLimit,
		encoder:       encoder,
	}, nil
}

var ErrBackupDirectoryInvalid = errors.New("backup directory is invalid")

// prepareBackupDirectory creates backup directory if it not exists.
// returns error is dir already exits and it is not empty.
func prepareBackupDirectory(dir string) error {
	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return makeDir(dir)
		}

		return err
	}

	if !dirInfo.IsDir() {
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

// forcePrepareBackupDirectory removes any existing directory and its contents and creates a new directory.
func forcePrepareBackupDirectory(dir string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return fmt.Errorf("%w: failed to remove directory %s: %v", ErrBackupDirectoryInvalid, dir, err)
	}

	return makeDir(dir)
}

func makeDir(dir string) error {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return fmt.Errorf("%w: failed to create backup directory %s: %v", ErrBackupDirectoryInvalid, dir, err)
	}

	return nil
}

// NewWriter creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The file is returned in write mode.
// If the fileSizeLimit is greater than 0, the file is wrapped in a Sized writer.
func (f *DirectoryWriterFactory) NewWriter(namespace string, writeHeader func(io.WriteCloser) error) (
	io.WriteCloser, error) {
	// open is a function that is executed for every file when split by size.
	open := func() (io.WriteCloser, error) {
		fileName := f.encoder.GenerateFilename(namespace, f.fileID.Add(1))
		filePath := filepath.Join(f.directory, fileName)

		file, err := openBackupFile(filePath)
		if err != nil {
			return nil, err
		}

		err = writeHeader(file)
		if err != nil {
			return nil, err
		}

		return file, err
	}

	writer, err := open()
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file: %w", err)
	}

	if f.fileSizeLimit > 0 {
		writer = writers.NewSized(f.fileSizeLimit, writer, open)
	}

	return writer, nil
}

func openBackupFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o666)
}

func (f *DirectoryWriterFactory) GetType() string {
	return "directory"
}
