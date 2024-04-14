package backup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/internal/writers"
)

type DirectoryWriterFactory struct {
	fileID    *atomic.Int32
	encoder   encoding.Encoder
	directory string

	fileSizeLimit int64
}

// NewDirectoryWriterFactory creates new factory for directory backups
// dir is target folder for backup
// fileSizeLimit is the maximum size of each backup file in bytes.
// If FileSizeLimit is 0, backup file size is unbounded.
// If non-zero, backup files will be split into multiple files if their size exceeds this limit.
// If non-zero, FileSizeLimit must be greater than or equal to 1MB.
// FileSizeLimit is not a strict limit, the actual file size may exceed this limit by a small amount.
func NewDirectoryWriterFactory(dir string, fileSizeLimit int64, encoder encoding.Encoder,
) (*DirectoryWriterFactory, error) {
	if fileSizeLimit > 0 && fileSizeLimit < 1024*1024 {
		return nil, fmt.Errorf("file size limit must be 0 for no limit, or at least 1MB, got %d", fileSizeLimit)
	}

	if fileSizeLimit < 0 {
		return nil, fmt.Errorf("file size limit must not be negative, got %d", fileSizeLimit)
	}

	err := prepareBackupDirectory(dir)
	if err != nil {
		return nil, err
	}

	return &DirectoryWriterFactory{
		directory:     dir,
		fileID:        &atomic.Int32{},
		fileSizeLimit: fileSizeLimit,
		encoder:       encoder,
	}, nil
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

// NewWriter creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The file is returned in write mode.
// If the fileSizeLimit is greater than 0, the file is wrapped in a Sized writer.
func (f *DirectoryWriterFactory) NewWriter(namespace string) (io.WriteCloser, error) {
	var open func() (io.WriteCloser, error)

	if _, ok := f.encoder.(*asb.Encoder); ok {
		open = func() (io.WriteCloser, error) {
			return f.getNewBackupFileASB(namespace, int(f.fileID.Add(1)))
		}
	} else {
		open = func() (io.WriteCloser, error) {
			return f.getNewBackupFileGeneric(namespace, int(f.fileID.Add(1)))
		}
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

// getNewBackupFileGeneric creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The files are returned in write mode.
func (f *DirectoryWriterFactory) getNewBackupFileGeneric(namespace string, id int) (io.WriteCloser, error) {
	fileName := getBackupFileNameGeneric(namespace, id)
	filePath := filepath.Join(f.directory, fileName)

	return openBackupFile(filePath)
}

// getNewBackupFileASB creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The files is returned in write mode.
// The file is created with an ASB header and .asb extension.
func (f *DirectoryWriterFactory) getNewBackupFileASB(namespace string, id int) (io.WriteCloser, error) {
	fileName := getBackupFileNameASB(namespace, id)
	filePath := filepath.Join(f.directory, fileName)

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
