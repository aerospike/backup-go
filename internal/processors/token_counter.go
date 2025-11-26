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

// TokenCounter count processed tokens. It's like a record counter but for XDR.
// This counter will count the number of received XDR payloads.
type TokenCounter[T models.TokenConstraint] struct {
	counter *atomic.Uint64
}

// NewTokenCounter returns new token counter.
func NewTokenCounter[T models.TokenConstraint](counter *atomic.Uint64) processor[T] {
	return &TokenCounter[T]{
		counter: counter,
	}
}

// Process counts the number of processed tokens.
func (c TokenCounter[T]) Process(token T) (T, error) {
	c.counter.Add(1)

	return token, nil
}
