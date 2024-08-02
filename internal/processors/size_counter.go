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

type sizeCounter struct {
	counter *atomic.Uint64
}

func NewSizeCounter(counter *atomic.Uint64) TokenProcessor {
	return &sizeCounter{
		counter: counter,
	}
}

func (c sizeCounter) Process(token *models.Token) (*models.Token, error) {
	c.counter.Add(token.Size)

	return token, nil
}
