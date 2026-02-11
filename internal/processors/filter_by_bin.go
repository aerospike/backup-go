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
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/util/collections"
	"github.com/aerospike/backup-go/models"
)

// filterByBin will remove bins with names in binsToRemove from every record it receives.
type filterByBin[T models.TokenConstraint] struct {
	binsToRemove map[string]bool
	skipped      *atomic.Uint64
}

// NewFilterByBin creates new filterByBin processor with given binList.
func NewFilterByBin[T models.TokenConstraint](binList []string, skipped *atomic.Uint64) Processor[T] {
	return &filterByBin[T]{
		binsToRemove: collections.ListToMap(binList),
		skipped:      skipped,
	}
}

// Process removes bins from records if they are in the binsToRemove list.
// If the record has no bins left after removal, it is filtered out.
func (p filterByBin[T]) Process(token T) (T, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type %T for filter by bin", token)
	}
	// if the token is not a record, we don't need to process it
	if t.Type != models.TokenTypeRecord {
		return token, nil
	}

	// If the filter bin list is not empty, perform filtering.
	if len(p.binsToRemove) > 0 {
		for key := range t.Record.Bins {
			if !p.binsToRemove[key] {
				delete(t.Record.Bins, key)
			}
		}
	}

	if len(t.Record.Bins) == 0 {
		p.skipped.Add(1)
		return nil, models.ErrFilteredOut
	}

	return any(t).(T), nil
}
