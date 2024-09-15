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
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
)

// noopProcessor is a no-op implementation of a processor.
type noopProcessor[T any] struct{}

// Process just passes the token through for noopProcessor.
func (n *noopProcessor[T]) Process(token T) (T, error) {
	return token, nil
}

type TokenProcessor = pipeline.DataProcessor[*models.Token]