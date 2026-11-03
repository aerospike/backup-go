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
	"errors"
	"fmt"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseResultResponse_ResponseError(t *testing.T) {
	t.Parallel()

	const cmd = "restore-prepare:namespace=bar;job-id=job"

	tests := []struct {
		name     string
		response string
		wantCode types.ResultCode
	}{
		{
			name:     "code and message",
			response: "ERROR:22:restore-prepare: ns=bar holds truncate state - see the log to clear it",
			wantCode: types.FAIL_FORBIDDEN,
		},
		{name: "no code", response: "ERROR::unknown command", wantCode: 0},
		{name: "free text", response: "ERROR: command failed", wantCode: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseResultResponse(cmd, map[string]string{cmd: tt.response})

			var refusal *ResponseError
			require.ErrorAs(t, err, &refusal)
			assert.Equal(t, tt.wantCode, refusal.Code)
			require.ErrorIs(t, err, errclass.ErrAerospike)
			require.EqualError(t, err, "aerospike error: command "+cmd+" failed: "+tt.response)
		})
	}
}

func TestFinalIfRefused(t *testing.T) {
	t.Parallel()

	refused := func(code string) error {
		return fmt.Errorf("failed prepare restore: %w", newResponseError("cmd", "ERROR:"+code+":refused"))
	}

	tests := []struct {
		name      string
		err       error
		wantFinal bool
	}{
		{name: "listed code", err: refused("22"), wantFinal: true},
		{name: "other code", err: refused("11")},
		{name: "not a refusal", err: errors.New("i/o timeout")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := finalIfRefused(tt.err, types.PARAMETER_ERROR, types.FAIL_FORBIDDEN)

			assert.Equal(t, tt.wantFinal, errors.As(err, new(finalError)))
			require.ErrorIs(t, err, tt.err)
		})
	}
}

func TestExecuteWithRetry_FinalErrorEndsRetries(t *testing.T) {
	t.Parallel()

	// The policy's MaxRetries counts every attempt, the first one included.
	const maxAttempts = 3

	refusal := newResponseError("cmd", "ERROR:22:refused")

	tests := []struct {
		name         string
		err          error
		wantAttempts int
	}{
		{name: "final error is not retried", err: finalError{err: refusal}, wantAttempts: 1},
		{name: "other error is retried", err: refusal, wantAttempts: maxAttempts},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			attempts := 0
			err := executeWithRetry(t.Context(), models.NewRetryPolicy(0, 1, maxAttempts), func() error {
				attempts++
				return tt.err
			})

			assert.Equal(t, tt.wantAttempts, attempts)
			require.ErrorIs(t, err, refusal)
		})
	}
}
