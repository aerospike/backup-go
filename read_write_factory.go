package backup

import "io"

type WriteFactory interface {
	NewWriter(namespace string) (io.WriteCloser, error)
}

type ReaderFactory interface {
	Readers() ([]io.ReadCloser, error) //TODO: use lazy creation
}
