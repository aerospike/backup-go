package processors

import (
	"sync/atomic"

	"github.com/aerospike/backup-go/logic/util"
	"github.com/aerospike/backup-go/models"
)

// setFilterProcessor filter records by set.
type setFilterProcessor struct {
	setsToRestore map[string]bool
	skipped       *atomic.Uint64
}

// NewProcessorSetFilter creates new setFilterProcessor with given setList.
func NewProcessorSetFilter(setList []string, skipped *atomic.Uint64) TokenProcessor {
	return &setFilterProcessor{
		setsToRestore: util.ListToMap(setList),
		skipped:       skipped,
	}
}

// Process filters out records that does not belong to setsToRestore
func (b setFilterProcessor) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	// if filter set list is empty, don't filter anything.
	if len(b.setsToRestore) == 0 {
		return token, nil
	}

	set := token.Record.Key.SetName()
	if b.setsToRestore[set] {
		return token, nil
	}

	b.skipped.Add(1)

	return nil, errFilteredOut
}
