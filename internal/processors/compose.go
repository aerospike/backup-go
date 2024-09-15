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

import "github.com/aerospike/backup-go/pipeline"

// composeProcessor is a no-op implementation of a processor.
type composeProcessor[T any] struct {
	processors []pipeline.DataProcessor[T]
}

// NewComposeProcessor creates a new ComposeProcessor with the given processors.
func NewComposeProcessor[T any](processors ...pipeline.DataProcessor[T]) pipeline.DataProcessor[T] {
	return &composeProcessor[T]{
		processors: processors,
	}
}

// Process applies all composed processors in sequence.
func (cp *composeProcessor[T]) Process(data T) (T, error) {
	var err error
	for _, processor := range cp.processors {
		data, err = processor.Process(data)
		if err != nil {
			return data, err
		}
	}

	return data, nil
}
