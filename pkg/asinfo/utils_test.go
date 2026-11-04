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

package asinfo

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo/mocks"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
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
		noWarn  bool
		want    []*models.SIndex
		wantErr bool
	}{
		{
			name:   "skips invalid indextype and keeps valid sindex",
			resp:   validSIndex + ";" + invalidSIndexType,
			noWarn: false,
			want:   []*models.SIndex{valid},
		},
		{
			name:   "skips sindex with invalid indextype only",
			resp:   invalidSIndexType,
			noWarn: false,
			want:   []*models.SIndex{},
		},
		{
			name:   "skips empty sindex entry",
			resp:   validSIndex + ";;" + invalidSIndexType,
			noWarn: false,
			want:   []*models.SIndex{valid},
		},
		{
			name:    "returns error for invalid bin type",
			resp:    "ns=test:set=testset:indexname=testindex:bin=testbin:type=BADTYPE:indextype=default:context=null:state=RW",
			noWarn:  false,
			wantErr: true,
		},
		{
			name:   "silently skips invalid indextype when noWarn is true",
			resp:   invalidSIndexType,
			noWarn: true,
			want:   []*models.SIndex{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockNodeGetter := mocks.NewMockNodeGetter(t)
			ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

			got, err := ic.parseSIndexes(tt.resp, tt.noWarn)
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

	const invalidSIndexType = "ns=test:set=testset:indexname=badindex:bin=testbin:type=numeric:indextype=badtype:context=null:state=RW"

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())
	ic.logger = logger

	got, err := ic.parseSIndexes(invalidSIndexType, false)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Contains(t, logBuf.String(), "skipping sindex with invalid type")
	require.Contains(t, logBuf.String(), "badindex")
	require.Contains(t, logBuf.String(), "badtype")

	logBuf.Reset()

	got, err = ic.parseSIndexes(invalidSIndexType, true)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, logBuf.String())
}

// testErrNoCode is an error response that carries no result code.
const testErrNoCode = "ERROR::failed"

func Test_cmdRespErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give string
		want int
	}{
		{name: "not found", give: testNoRestoreJob, want: cmdRespCodeNotFound},
		{name: "leading space", give: " " + testNoRestoreJob, want: cmdRespCodeNotFound},
		{name: "parameter error", give: "ERROR:4:'nodes' is not a restore parameter", want: 4},
		{name: "code without message", give: "ERROR:22", want: 22},
		{name: "no code", give: testErrNoCode, want: cmdRespNoCode},
		{name: "not a number", give: "ERROR:abc:failed", want: cmdRespNoCode},
		{name: "no prefix", give: "ok", want: cmdRespNoCode},
		{name: "prefix without separator", give: errCmdRespPrefix, want: cmdRespNoCode},
		{name: "error code inside a row", give: "error-reason=S3 " + testNoRestoreJob, want: cmdRespNoCode},
		{
			name: "message with colons",
			give: "ERROR:22:restore-abort: wrong namespace - job-id=r1 is for ns=test",
			want: 22,
		},
		{
			name: "start not acknowledged in time",
			give: "ERROR:9:backup job b1: start not acknowledged by all nodes and could not be confirmed",
			want: cmdRespCodeTimeout,
		},
		{
			name: "start not confirmed",
			give: "ERROR:1:backup job b1: start could not be confirmed - check backup-status before retrying",
			want: cmdRespCodeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, cmdRespErrorCode(tt.give))
		})
	}
}

func Test_isNotFoundResp(t *testing.T) {
	t.Parallel()

	const testCmd = testRestoreStatusCmd

	respErr := func(resp string) error {
		_, err := parseResultResponse(testCmd, map[string]string{testCmd: resp})

		return fmt.Errorf("failed to request restore status: %w", err)
	}

	tests := []struct {
		name string
		give error
		want bool
	}{
		{name: "not found response", give: respErr(testNoRestoreJob), want: true},
		{name: "no jobs at all", give: respErr("ERROR:2:no backup jobs"), want: true},
		{name: "other error response", give: respErr("ERROR:20:unknown namespace source-ns"), want: false},
		{name: "network error", give: fmt.Errorf("%w: network", errclass.ErrAerospike), want: false},
		{name: "nil", give: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isNotFoundResp(tt.give))

			if tt.give != nil {
				assert.ErrorIs(t, tt.give, errclass.ErrAerospike)
			}
		})
	}
}

func Test_mayHaveTakenEffect(t *testing.T) {
	t.Parallel()

	const testCmd = testRestoreStatusCmd

	respErr := func(resp string) error {
		_, err := parseResultResponse(testCmd, map[string]string{testCmd: resp})

		return fmt.Errorf("failed to start restore: %w", err)
	}

	tests := []struct {
		name string
		give error
		want bool
	}{
		{name: "network error", give: fmt.Errorf("%w: network", errclass.ErrAerospike), want: true},
		{name: "not acknowledged in time", give: respErr("ERROR:9:restore: composite publish timed out"), want: true},
		{
			name: "start not confirmed",
			give: respErr("ERROR:1:backup job b1: start could not be confirmed"),
			want: true,
		},
		{name: "parameter error", give: respErr("ERROR:4:missing 'job-id'"), want: false},
		{name: "job id in use", give: respErr("ERROR:22:backup: job-id=b1 is in use or was used before"), want: false},
		{
			name: "job id taken at publish time",
			give: respErr("ERROR:26:backup job b1: this job-id is in use - poll backup-status, or pick another job-id"),
			want: false,
		},
		{name: "error without a code", give: respErr(testErrNoCode), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, mayHaveTakenEffect(tt.give))
		})
	}
}

func Test_principalFromStatistics(t *testing.T) {
	t.Parallel()

	const (
		testPrincipal      = "BB9020011AC4202"
		testOtherPrincipal = "BB9030011AC4202"
	)

	tests := []struct {
		name    string
		give    []infomodels.InfoMap
		want    string
		wantErr bool
	}{
		{
			name: "paxos principal",
			give: []infomodels.InfoMap{{statPaxosPrincipal: testPrincipal}},
			want: testPrincipal,
		},
		{
			name: "paxos principal is preferred",
			give: []infomodels.InfoMap{{statClusterPrincipal: testOtherPrincipal, statPaxosPrincipal: testPrincipal}},
			want: testPrincipal,
		},
		{
			name: "cluster principal when paxos principal is missing",
			give: []infomodels.InfoMap{{statClusterPrincipal: testPrincipal}},
			want: testPrincipal,
		},
		{name: "no principal", give: []infomodels.InfoMap{{"cluster_size": "3"}}, wantErr: true},
		{name: "empty principal", give: []infomodels.InfoMap{{statPaxosPrincipal: ""}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := principalFromStatistics(tt.give)
			if tt.wantErr {
				require.ErrorIs(t, err, errclass.ErrAerospike)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
