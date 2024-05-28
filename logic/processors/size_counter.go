package processors

import (
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
)

type sizeCounter struct {
	counter *atomic.Uint64
}

func NewSizeCounter(counter *atomic.Uint64) TokenProcessor {
	return &sizeCounter{
		counter: counter,
	}
}

func (c sizeCounter) Process(token *models.Token) (*models.Token, error) {
	c.counter.Add(token.Size)

	return token, nil
}
