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
	"math"
	"testing"

	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/require"
)

func TestValidateFileLimit(t *testing.T) {
	t.Parallel()

	const maxChunks = 10_000

	tests := []struct {
		name      string
		fileLimit uint64
		setupMock func(t *testing.T) Writer
		wantErr   string
	}{
		{
			name:      "zero file limit skips validation",
			fileLimit: 0,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				// no expectations
				return mocks.NewMockWriter(t)
			},
		},
		{
			name:      "nil writer skips validation",
			fileLimit: 100,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				return nil
			},
		},
		{
			name:      "local writer skips validation",
			fileLimit: 100,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return(local.TypeLocal).Once()
				return m
			},
		},
		{
			name:      "zero chunk size returns error",
			fileLimit: 1,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 0}).Once()
				return m
			},
			wantErr: "chunk size must be positive, got 0",
		},
		{
			name:      "negative chunk size returns error",
			fileLimit: 1,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: -1}).Once()
				return m
			},
			wantErr: "chunk size must be positive, got -1",
		},
		{
			name:      "valid: single chunk",
			fileLimit: 100,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 1000}).Once()
				return m
			},
		},
		{
			name:      "valid: exact chunk boundary",
			fileLimit: 1000,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				// 10 chunks
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 100}).Once()
				return m
			},
		},
		{
			name:      "valid: non-divisible rounds up",
			fileLimit: 101,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				// it works as ceil(101/100) = 2
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 100}).Once()
				return m
			},
		},
		{
			name:      "valid: just below max chunks",
			fileLimit: uint64(maxChunks-1) * 100,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				// 9999 chunks
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 100}).Once()
				return m
			},
		},
		{
			name:      "valid: large chunk size keeps chunks low",
			fileLimit: math.MaxUint64,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: math.MaxInt64}).Once()
				return m
			},
		},
		{
			name:      "invalid: exactly max chunks",
			fileLimit: uint64(maxChunks) * 100,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				// 10000 chunks
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 100}).Once()
				return m
			},
			wantErr: "exceeds maximum of 10000",
		},
		{
			name:      "invalid: exceeds max chunks",
			fileLimit: uint64(maxChunks+1) * 100,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 100}).Once()
				return m
			},
			wantErr: "exceeds maximum of 10000",
		},
		{
			name:      "invalid: max uint64 with chunk size 1",
			fileLimit: math.MaxUint64,
			setupMock: func(t *testing.T) Writer {
				t.Helper()
				m := mocks.NewMockWriter(t)
				m.EXPECT().GetType().Return("s3").Once()
				m.EXPECT().GetOptions().Return(options.Options{ChunkSize: 1}).Once()
				return m
			},
			wantErr: "exceeds maximum of 10000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateFileLimit(tt.fileLimit, tt.setupMock(t))

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
