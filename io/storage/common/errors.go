// Copyright 2024-2026 Aerospike, Inc.
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

package common

import (
	"context"
	"fmt"

	"github.com/aerospike/backup-go/models"
)

var (
	// ErrEmptyStorage describes the empty storage error for the restore operation.
	// It belongs to the [models.ErrNotFound] class.
	ErrEmptyStorage = fmt.Errorf("%w: empty storage", models.ErrNotFound)

	// ErrArchivedObject is returned for an object that is archived and must be
	// restored by the user before it can be read.
	// It belongs to the [models.ErrStorage] class.
	ErrArchivedObject = fmt.Errorf("%w: archived object", models.ErrStorage)
)

// ErrToChan checks context before sending an error to errors chan.
// If context is already canceled and the reader must be stopped, no need to send error to errors chan.
func ErrToChan(ctx context.Context, ch chan<- error, err error) {
	if err != nil && ctx.Err() == nil {
		select {
		case ch <- err:
		case <-ctx.Done():
		}
	}
}
