package backup

import (
	"io"

	"github.com/aerospike/backup-go/internal/logging"
)

type WriteFactory interface {
	NewWriter(namespace string) (io.WriteCloser, error)
	GetType() logging.HandlerType
}

type ReaderFactory interface {
	Readers() ([]io.ReadCloser, error) //TODO: use lazy creation
	GetType() logging.HandlerType
}
