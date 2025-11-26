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

	"github.com/aerospike/backup-go/models"
)

// sizeCounter counts the size of tokens.
type sizeCounter[T models.TokenConstraint] struct {
	counter *atomic.Uint64
}

// NewSizeCounter creates a new sizeCounter processor.
func NewSizeCounter[T models.TokenConstraint](counter *atomic.Uint64) processor[T] {
	return &sizeCounter[T]{
		counter: counter,
	}
}

// Process aggregates the size of a token.
func (c sizeCounter[T]) Process(token T) (T, error) {
	c.counter.Add(token.GetSize())
	return token, nil
}
