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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testDC           = "dc"
	testLocalAddress = "127.0.0.1"
	testTimeout      = 1000
	testSize         = 100
)

func Test_validateBackupXDR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		backup  *BackupXDR
		wantErr string
	}{
		{
			name: "valid backup",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      testTimeout,
				WriteTimeoutMilliseconds:     testTimeout,
				InfoPolingPeriodMilliseconds: testTimeout,
				StartTimeoutMilliseconds:     testTimeout,
				ResultQueueSize:              testSize,
				AckQueueSize:                 testSize,
				MaxConnections:               10,
				ParallelWrite:                5,
				FileLimit:                    testTimeout,
			},
			wantErr: "",
		},
		{
			name: "no namespace",
			backup: &BackupXDR{
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      testTimeout,
				WriteTimeoutMilliseconds:     testTimeout,
				InfoPolingPeriodMilliseconds: testTimeout,
				StartTimeoutMilliseconds:     testTimeout,
				ResultQueueSize:              testSize,
				AckQueueSize:                 testSize,
				MaxConnections:               10,
				ParallelWrite:                5,
				FileLimit:                    testTimeout,
			},
			wantErr: "namespace is required",
		},
		{
			name: "no dc",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      testTimeout,
				WriteTimeoutMilliseconds:     testTimeout,
				InfoPolingPeriodMilliseconds: testTimeout,
				StartTimeoutMilliseconds:     testTimeout,
				ResultQueueSize:              testSize,
				AckQueueSize:                 testSize,
				MaxConnections:               10,
				ParallelWrite:                5,
				FileLimit:                    testTimeout,
			},
			wantErr: "dc is required",
		},
		{
			name: "no local address",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				ReadTimeoutMilliseconds:      testTimeout,
				WriteTimeoutMilliseconds:     testTimeout,
				InfoPolingPeriodMilliseconds: testTimeout,
				StartTimeoutMilliseconds:     testTimeout,
				ResultQueueSize:              testSize,
				AckQueueSize:                 testSize,
				MaxConnections:               10,
				ParallelWrite:                5,
				FileLimit:                    testTimeout,
			},
			wantErr: "local address is required",
		},
		{
			name: "negative read timeout",
			backup: &BackupXDR{
				Namespace:               testNamespace,
				DC:                      testDC,
				LocalAddress:            testLocalAddress,
				ReadTimeoutMilliseconds: -1,
			},
			wantErr: "backup xdr read timeout can't be negative",
		},
		{
			name: "negative write timeout",
			backup: &BackupXDR{
				Namespace:                testNamespace,
				DC:                       testDC,
				LocalAddress:             testLocalAddress,
				ReadTimeoutMilliseconds:  0,
				WriteTimeoutMilliseconds: -1,
			},
			wantErr: "backup xdr write timeout can't be negative",
		},
		{
			name: "negative info polling period",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: -1,
			},
			wantErr: "backup xdr info poling period can't be negative",
		},
		{
			name: "negative start timeout",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     -1,
			},
			wantErr: "backup xdr start timeout can't be negative",
		},
		{
			name: "negative result queue size",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              -1,
			},
			wantErr: "backup xdr result queue size can't be negative",
		},
		{
			name: "negative ack queue size",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              0,
				AckQueueSize:                 -1,
			},
			wantErr: "backup xdr ack queue size can't be negative",
		},
		{
			name: "invalid max connections",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              0,
				AckQueueSize:                 0,
				MaxConnections:               0,
			},
			wantErr: "backup xdr max connections can't be less than 1",
		},
		{
			name: "invalid file limit",
			backup: &BackupXDR{
				Namespace:                    testNamespace,
				DC:                           testDC,
				LocalAddress:                 testLocalAddress,
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              0,
				AckQueueSize:                 0,
				MaxConnections:               1,
				ParallelWrite:                1,
				FileLimit:                    0,
			},
			wantErr: "backup xdr file limit can't be less than 1",
		},
		{
			name: "negative info retry interval",
			backup: &BackupXDR{
				Namespace:                     testNamespace,
				DC:                            testDC,
				LocalAddress:                  testLocalAddress,
				MaxConnections:                1,
				ParallelWrite:                 1,
				FileLimit:                     1,
				InfoRetryIntervalMilliseconds: -1,
			},
			wantErr: "backup xdr info retry interval can't be negative",
		},
		{
			name: "negative info retries multiplier",
			backup: &BackupXDR{
				Namespace:             testNamespace,
				DC:                    testDC,
				LocalAddress:          testLocalAddress,
				MaxConnections:        1,
				ParallelWrite:         1,
				FileLimit:             1,
				InfoRetriesMultiplier: -1,
			},
			wantErr: "backup xdr info retries multiplier can't be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.backup.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}
