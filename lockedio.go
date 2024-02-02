package backuplib

import (
	"io"
	"sync"
)

type LockedReader struct {
	io.Reader
	lock *sync.Mutex
}

func NewLockedReader(r io.Reader) *LockedReader {
	return &LockedReader{
		Reader: r,
		lock:   &sync.Mutex{},
	}
}

func (r *LockedReader) Read(p []byte) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.Reader.Read(p)
}
