// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
