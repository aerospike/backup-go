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

package asinfo

import (
	"bytes"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo/mocks"
	"github.com/stretchr/testify/require"
)

func TestClient_parseSIndexes_skipsInvalidIndexType(t *testing.T) {
	t.Parallel()

	const (
		validSIndex       = "ns=test:set=testset:indexname=validindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW"
		invalidSIndexType = "ns=test:set=testset:indexname=badindex:bin=testbin:type=numeric:indextype=badtype:context=null:state=RW"
	)

	valid := &models.SIndex{
		Namespace: "test",
		Name:      "validindex",
		Set:       "testset",
		Path: models.SIndexPath{
			BinName: "testbin",
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	tests := []struct {
		name    string
		resp    string
		want    []*models.SIndex
		wantErr bool
	}{
		{
			name: "skips invalid indextype and keeps valid sindex",
			resp: validSIndex + ";" + invalidSIndexType,
			want: []*models.SIndex{valid, nil},
		},
		{
			name: "skips sindex with invalid indextype only",
			resp: invalidSIndexType,
			want: []*models.SIndex{nil},
		},
		{
			name: "skips empty sindex entry",
			resp: validSIndex + ";;" + invalidSIndexType,
			want: []*models.SIndex{valid, nil, nil},
		},
		{
			name:    "returns error for invalid bin type",
			resp:    "ns=test:set=testset:indexname=testindex:bin=testbin:type=BADTYPE:indextype=default:context=null:state=RW",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockNodeGetter := mocks.NewMockNodeGetter(t)
			ic := newClient(mockNodeGetter, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

			got, err := ic.parseSIndexes(tt.resp)
			if tt.wantErr {
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrInvalidSIndexType)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestClient_parseSIndexes_logsSkippedInvalidIndexType(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	ic := newClient(mockNodeGetter, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())
	ic.logger = logger

	resp := "ns=test:set=testset:indexname=badindex:bin=testbin:type=numeric:indextype=badtype:context=null:state=RW"
	got, err := ic.parseSIndexes(resp)
	require.NoError(t, err)
	require.Equal(t, []*models.SIndex{nil}, got)
	require.Contains(t, logBuf.String(), "skipping sindex with invalid type")
	require.Contains(t, logBuf.String(), "badindex")
}
