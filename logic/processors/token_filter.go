package processors

import (
	"fmt"

	"github.com/aerospike/backup-go/models"
)

// tokenTypeFilterProcessor is used to support no-records, no-indexes and no-udf flags.
type tokenTypeProcessor struct {
	noRecords bool
	noIndexes bool
	noUdf     bool
}

// NewTokenTypeFilterProcessor creates new tokenTypeFilterProcessor
func NewTokenTypeFilterProcessor(noRecords, noIndexes, noUdf bool) TokenProcessor {
	if !noRecords && !noIndexes && !noUdf {
		return &noopProcessor[*models.Token]{}
	}

	return &tokenTypeProcessor{
		noRecords: noRecords,
		noIndexes: noIndexes,
		noUdf:     noUdf,
	}
}

// Process filters tokens by type.
func (b tokenTypeProcessor) Process(token *models.Token) (*models.Token, error) {
	if b.noRecords && token.Type == models.TokenTypeRecord {
		return nil, fmt.Errorf("%w: record is filtered with no-records flag", errFilteredOut)
	}

	if b.noIndexes && token.Type == models.TokenTypeSIndex {
		return nil, fmt.Errorf("%w: index is filtered with no-indexes flag", errFilteredOut)
	}

	if b.noUdf && token.Type == models.TokenTypeUDF {
		return nil, fmt.Errorf("%w: udf is filtered with no-udf flag", errFilteredOut)
	}

	return token, nil
}
