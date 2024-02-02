package input

import (
	"io"
	"os"
)

type File struct {
	path string
	io.ReadCloser
}

// NewFile returns a new File instance
func NewFile(path string) (*File, error) {
	return &File{
		path:       path,
		ReadCloser: nil,
	}, nil
}

// Open opens the file at the path
func (f *File) Open() (*File, error) {
	osf, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}

	f.ReadCloser = osf
	return f, nil
}

func (f *File) Close() error {
	return f.ReadCloser.Close()
}
