package processors

import (
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
)

type recordCounter struct {
	counter *atomic.Uint64
}

func NewRecordCounter(counter *atomic.Uint64) TokenProcessor {
	return &recordCounter{
		counter: counter,
	}
}

func (c recordCounter) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	c.counter.Add(1)

	return token, nil
}
