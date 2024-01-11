package output

import (
	"io"
	"sync"
)

type LockedFile struct {
	File
	lock *sync.Mutex
}

func NewLockedFile(path string) *LockedFile {
	return &LockedFile{
		File: *NewFile(path),
		lock: &sync.Mutex{},
	}
}

func (f *LockedFile) Open() (io.WriteCloser, error) {
	wc, err := f.File.Open()
	if err != nil {
		return nil, err
	}

	return &lockedWriteCloser{
		lock:        f.lock,
		WriteCloser: wc,
	}, nil
}

func (f *LockedFile) Resume() (io.WriteCloser, error) {
	wc, err := f.File.Resume()
	if err != nil {
		return nil, err
	}

	return &lockedWriteCloser{
		lock:        f.lock,
		WriteCloser: wc,
	}, nil
}

type lockedWriteCloser struct {
	lock *sync.Mutex
	io.WriteCloser
}

func (o *lockedWriteCloser) Write(p []byte) (int, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.WriteCloser.Write(p)
}

func (o *lockedWriteCloser) Close() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	return o.WriteCloser.Close()
}
