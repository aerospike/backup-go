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

// filterBySet filter records by set.
type filterBySet[T models.TokenConstraint] struct {
	setsToRestore map[string]bool
	skipped       *atomic.Uint64
}

// NewFilterBySet creates new filterBySet processor with given setList.
func NewFilterBySet[T models.TokenConstraint](setList []string, skipped *atomic.Uint64) Processor[T] {
	return &filterBySet[T]{
		setsToRestore: collections.ListToMap(setList),
		skipped:       skipped,
	}
}

// Process filters out records that do not belong to setsToRestore.
func (p filterBySet[T]) Process(token T) (T, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type %T for filter by set", token)
	}
	// if the token is not a record, we don't need to process it
	if t.Type != models.TokenTypeRecord {
		return token, nil
	}

	// if filter set list is empty, don't filter anything.
	if len(p.setsToRestore) == 0 {
		return token, nil
	}

	set := t.Record.Key.SetName()
	if p.setsToRestore[set] {
		return token, nil
	}

	p.skipped.Add(1)

	return nil, models.ErrFilteredOut
}
