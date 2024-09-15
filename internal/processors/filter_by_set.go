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
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/util"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
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

	return nil, pipeline.ErrFilteredOut
}
