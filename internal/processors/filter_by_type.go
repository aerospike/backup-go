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
type filterByType[T models.TokenConstraint] struct {
	noRecords bool
	noIndexes bool
	noUdf     bool
}

// NewFilterByType creates new filterByType processor
func NewFilterByType[T models.TokenConstraint](noRecords, noIndexes, noUdf bool) processor[T] {
	if !noRecords && !noIndexes && !noUdf {
		return &noopProcessor[T]{}
	}

	return &filterByType[T]{
		noRecords: noRecords,
		noIndexes: noIndexes,
		noUdf:     noUdf,
	}
}

// Process filters tokens by type.
func (p filterByType[T]) Process(token T) (T, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type %T for record counter", token)
	}

	if p.noRecords && t.Type == models.TokenTypeRecord {
		return nil, fmt.Errorf("%w: record is filtered with no-records flag", models.ErrFilteredOut)
	}

	if p.noIndexes && t.Type == models.TokenTypeSIndex {
		return nil, fmt.Errorf("%w: index is filtered with no-indexes flag", models.ErrFilteredOut)
	}

	if p.noUdf && t.Type == models.TokenTypeUDF {
		return nil, fmt.Errorf("%w: udf is filtered with no-udf flag", models.ErrFilteredOut)
	}

	return token, nil
}
