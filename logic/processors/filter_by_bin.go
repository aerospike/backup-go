package processors

import (
	"sync/atomic"

	"github.com/aerospike/backup-go/logic/util"
	"github.com/aerospike/backup-go/models"
)

// filterByBin will remove bins with names in binsToRemove from every record it receives.
type filterByBin struct {
	binsToRemove map[string]bool
	skipped      *atomic.Uint64
}

// NewFilterByBin creates new filterByBin processor with given binList.
func NewFilterByBin(binList []string, skipped *atomic.Uint64) TokenProcessor {
	return &filterByBin{
		binsToRemove: util.ListToMap(binList),
		skipped:      skipped,
	}
}

func (p filterByBin) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	// if filter bin list is empty, don't filter anything.
	if len(p.binsToRemove) == 0 {
		return token, nil
	}

	for key := range token.Record.Bins {
		if !p.binsToRemove[key] {
			delete(token.Record.Bins, key)
		}
	}

	if len(token.Record.Bins) == 0 {
		p.skipped.Add(1)
		return nil, errFilteredOut
	}

	return token, nil
}
