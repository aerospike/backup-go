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

package models

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// PartitionFilterSerialized represent serialized a.PartitionFilter.
// To save cursor state.
type PartitionFilterSerialized struct {
	Begin  int
	Count  int
	Digest []byte
	Cursor []byte
	// Worker number.
	N int
}

// NewPartitionFilterSerialized serialize *a.PartitionFilter and returns new PartitionFilterSerialized instance.
func NewPartitionFilterSerialized(pf *a.PartitionFilter) (PartitionFilterSerialized, error) {
	if pf == nil || pf.IsDone() {
		return PartitionFilterSerialized{}, nil
	}

	c, err := pf.EncodeCursor()
	if err != nil {
		return PartitionFilterSerialized{}, fmt.Errorf("failed to encode cursor: %w", err)
	}

	return PartitionFilterSerialized{
		Begin:  pf.Begin,
		Count:  pf.Count,
		Digest: pf.Digest,
		Cursor: c,
	}, nil
}

// Decode decodes *PartitionFilterSerialized to *a.PartitionFilter
func (p *PartitionFilterSerialized) Decode() (*a.PartitionFilter, error) {
	pf := &a.PartitionFilter{Begin: p.Begin, Count: p.Count, Digest: p.Digest}
	if err := pf.DecodeCursor(p.Cursor); err != nil {
		return nil, fmt.Errorf("failed to decode cursor: %w", err)
	}

	return pf, nil
}
