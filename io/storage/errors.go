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

package storage

import (
	"context"
	"errors"
)

// ErrEmptyStorage describes the empty storage error for the restore operation.
var (
	ErrEmptyStorage   = errors.New("empty storage")
	ErrArchivedObject = errors.New("archived object")
)

// ErrToChan checks context before sending an error to errors chan.
// If context is already canceled and the reader must be stopped, no need to send error to errors chan.
func ErrToChan(ctx context.Context, ch chan<- error, err error) {
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case ch <- err:
		}
	}
}
