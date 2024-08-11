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

func TestPartitionRange_validate(t *testing.T) {
	type fields struct {
		Begin int
		Count int
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Test positive PartitionRange validate",
			fields: fields{
				Begin: 0,
				Count: 5,
			},
			wantErr: false,
		},
		{
			name: "Test negative PartitionRange validate count 0",
			fields: fields{
				Begin: 5,
				Count: 0,
			},
			wantErr: true,
		},
		{
			name: "Test negative PartitionRange validate begin -1",
			fields: fields{
				Begin: -1,
				Count: 5,
			},
			wantErr: true,
		},
		{
			name: "Test negative PartitionRange validate count -1",
			fields: fields{
				Begin: 5,
				Count: -1,
			},
			wantErr: true,
		},
		{
			name: "Test negative PartitionRange validate total partitions greater than 4096",
			fields: fields{
				Begin: 4000,
				Count: 1000,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PartitionRange{
				Begin: tt.fields.Begin,
				Count: tt.fields.Count,
			}
			if err := p.validate(); (err != nil) != tt.wantErr {
				t.Errorf("PartitionRange.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

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
