package processors

import (
	"fmt"

	"github.com/aerospike/backup-go/models"
)

// tokenTypeFilterProcessor is used to support no-records, no-indexes and no-udf flags.
type filterByType struct {
	noRecords bool
	noIndexes bool
	noUdf     bool
}

// NewFilterByType creates new filterByType processor
func NewFilterByType(noRecords, noIndexes, noUdf bool) TokenProcessor {
	if !noRecords && !noIndexes && !noUdf {
		return &noopProcessor[*models.Token]{}
	}

	return &filterByType{
		noRecords: noRecords,
		noIndexes: noIndexes,
		noUdf:     noUdf,
	}
}

// Process filters tokens by type.
func (p filterByType) Process(token *models.Token) (*models.Token, error) {
	if p.noRecords && token.Type == models.TokenTypeRecord {
		return nil, fmt.Errorf("%w: record is filtered with no-records flag", errFilteredOut)
	}

	if p.noIndexes && token.Type == models.TokenTypeSIndex {
		return nil, fmt.Errorf("%w: index is filtered with no-indexes flag", errFilteredOut)
	}

	if p.noUdf && token.Type == models.TokenTypeUDF {
		return nil, fmt.Errorf("%w: udf is filtered with no-udf flag", errFilteredOut)
	}

	return token, nil
}
