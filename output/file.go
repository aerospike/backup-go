package output

import (
	"io"
	"os"
)

type File struct {
	path string
}

func NewFile(path string) *File {
	return &File{
		path: path,
	}
}

func (f *File) Open() (io.WriteCloser, error) {
	return os.OpenFile(f.path, os.O_RDWR, 0755)
}

func (f *File) Resume() (io.WriteCloser, error) {
	return os.OpenFile(f.path, os.O_APPEND, 0755)
}
