package processors

import (
	"sync/atomic"

	"github.com/aerospike/backup-go/logic/util"
	"github.com/aerospike/backup-go/models"
)

// filterBySet filter records by set.
type filterBySet struct {
	setsToRestore map[string]bool
	skipped       *atomic.Uint64
}

// NewFilterBySet creates new filterBySet processor with given setList.
func NewFilterBySet(setList []string, skipped *atomic.Uint64) TokenProcessor {
	return &filterBySet{
		setsToRestore: util.ListToMap(setList),
		skipped:       skipped,
	}
}

// Process filters out records that does not belong to setsToRestore
func (p filterBySet) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	// if filter set list is empty, don't filter anything.
	if len(p.setsToRestore) == 0 {
		return token, nil
	}

	set := token.Record.Key.SetName()
	if p.setsToRestore[set] {
		return token, nil
	}

	p.skipped.Add(1)

	return nil, errFilteredOut
}
