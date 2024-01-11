package output

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type WorkerID int

type fileStatus struct {
	path   string
	resume bool
}

type DirectoryHandler struct {
	directory  string
	filePrefix string
	data       map[WorkerID]*fileStatus
	count      uint64
	requests   chan WorkerID
	responses  chan *io.WriteCloser
}

func NewDirectoryHandler(directory, filePrefix string, workers int) *DirectoryHandler {
	files := make(map[WorkerID]*fileStatus)
	return &DirectoryHandler{
		directory:  directory,
		filePrefix: filePrefix,
		data:       files,
		requests:   make(chan WorkerID, workers),
		responses:  make(chan *io.WriteCloser, workers),
	}
}

func (o *DirectoryHandler) Resume(ctx context.Context) error {
	for _, f := range o.data {
		f.resume = true
	}

	return o.Run(ctx)
}

func (o *DirectoryHandler) Run(ctx context.Context) error {
	errc := make(chan error)

	go func(ctx context.Context) {
		select {
		case id := <-o.requests:
			curFile := o.getFileStatus(id)

			var w io.WriteCloser
			var err error
			if curFile.resume {
				w, err = o.open(id)
				if err != nil {
					curFile.resume = false
				}
			} else {
				w, err = o.openNew(id)
			}

			if err != nil {
				errc <- err
				return
			}

			o.responses <- &w

		case <-ctx.Done():
			return
		}
	}(ctx)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (o *DirectoryHandler) getFileStatus(id WorkerID) fileStatus {
	return *o.data[id]
}

func (o *DirectoryHandler) open(id WorkerID) (io.WriteCloser, error) {
	f := o.data[id]
	return os.OpenFile(f.path, os.O_RDWR, 0755)
}

//TODO make an append method for resuming

func (o *DirectoryHandler) openNew(id WorkerID) (io.WriteCloser, error) {
	newFileName := genFileName(o.directory, o.filePrefix, o.count)
	f := &fileStatus{
		path: newFileName,
	}
	o.set(id, f)
	return o.open(id)
}

func (o *DirectoryHandler) set(id WorkerID, out *fileStatus) error {
	o.count += 1
	o.data[id] = out
	return nil
}

func genFileName(directory, filePrefix string, fileID uint64) string {
	fileBase := filepath.Join(directory, filePrefix)
	return fmt.Sprintf("%s_%05d.asb", fileBase, fileID)
}
