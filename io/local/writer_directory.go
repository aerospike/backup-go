package local

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aerospike/backup-go"
)

type DirectoryWriterFactory struct {
	directory string
}

var _ backup.WriteFactory = (*DirectoryWriterFactory)(nil)

// NewDirectoryWriterFactory creates new factory for directory backups
// dir is target folder for backup
// fileSizeLimit is the maximum size of each backup file in bytes.
// If FileSizeLimit is 0, backup file size is unbounded.
// If non-zero, backup files will be split into multiple files if their size exceeds this limit.
// If non-zero, FileSizeLimit must be greater than or equal to 1MB.
// FileSizeLimit is not a strict limit, the actual file size may exceed this limit by a small amount.
func NewDirectoryWriterFactory(dir string, removeFiles bool,
) (*DirectoryWriterFactory, error) {
	var err error
	if removeFiles {
		err = forcePrepareBackupDirectory(dir)
	} else {
		err = prepareBackupDirectory(dir)
	}

	if err != nil {
		return nil, err
	}

	return &DirectoryWriterFactory{
		directory: dir,
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

type bufferedFile struct {
	*bufio.Writer
	file *os.File
}

func (bf *bufferedFile) Close() error {
	err := bf.Writer.Flush()
	if err != nil {
		return err
	}

	return bf.file.Close()
}

// NewWriter creates a new backup file in the given directory.
// The file name is based on the namespace and the id.
// The file is returned in write mode.
// If the fileSizeLimit is greater than 0, the file is wrapped in a Sized writer.
func (f *DirectoryWriterFactory) NewWriter(fileName string) (io.WriteCloser, error) {
	filePath := filepath.Join(f.directory, fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o666)

	if err != nil {
		return nil, err
	}

	return &bufferedFile{bufio.NewWriter(file), file}, nil
}

func (f *DirectoryWriterFactory) GetType() string {
	return "directory"
}
