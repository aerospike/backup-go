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

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
)

type recordCounter[T models.TokenConstraint] struct {
	counter *atomic.Uint64
}

func NewRecordCounter[T models.TokenConstraint](counter *atomic.Uint64) pipeline.DataProcessor[T] {
	return &recordCounter[T]{
		counter: counter,
	}
}

func (c recordCounter[T]) Process(token T) (T, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type for record counter")
	}
	// if the token is not a record, we don't need to process it
	if t.Type != models.TokenTypeRecord {
		return token, nil
	}

	c.counter.Add(1)

	return token, nil
}
