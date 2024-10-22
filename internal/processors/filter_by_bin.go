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

	// If the record contains no bins, we skip it.
	if len(token.Record.Bins) == 0 {
		p.skipped.Add(1)
		return nil, pipeline.ErrFilteredOut
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

	return token, nil
}
