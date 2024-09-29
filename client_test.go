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
	"log/slog"
	"strings"
	"testing"

	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func TestNilClient(t *testing.T) {
	_, err := NewClient(nil)
	assert.Error(t, err, "aerospike client is required")
}

func TestClientOptions(t *testing.T) {
	var logBuffer strings.Builder
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	sem := semaphore.NewWeighted(10)
	id := "ID"

	client, err := NewClient(&mocks.MockAerospikeClient{},
		WithID(id),
		WithLogger(logger),
		WithScanLimiter(sem),
	)

	assert.NoError(t, err)
	assert.Equal(t, id, client.id)
	assert.Equal(t, sem, client.scanLimiter)

	client.logger.Info("test")
	assert.Contains(t, logBuffer.String(), "level=INFO msg=test backup.client.id=ID")
}
