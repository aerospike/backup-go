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

package backup

import (
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

// dataProcessor is a processor that executes a list of processors in sequence.
type dataProcessor[T models.TokenConstraint] struct {
	execs []pipe.Processor[T]
}

// newDataProcessor returns a new data processor.
func newDataProcessor[T models.TokenConstraint](execs ...pipe.Processor[T]) pipe.ProcessorCreator[T] {
	return func() pipe.Processor[T] {
		return &dataProcessor[T]{
			execs: execs,
		}
	}
}

// Process executes the list of processors in sequence and returns the result
// of the last processor. If any processor returns an error, the error is returned.
func (p *dataProcessor[T]) Process(data T) (T, error) {
	var err error
	for _, processor := range p.execs {
		data, err = processor.Process(data)
		if err != nil {
			return data, err
		}
	}

	return data, nil
}
