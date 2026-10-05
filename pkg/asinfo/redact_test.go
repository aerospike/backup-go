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
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo/mocks"
	"github.com/stretchr/testify/require"
)

const (
	// Fake credential values, only used to assert they never reach logs or errors.
	testRedactAccessVal    = "access-value-fake-0123456789"
	testRedactSensitiveVal = "sensitive-value-fake-9876543210"
	testRedactNamespace    = "source-ns"
	testRedactBucket       = "backup-bucket"
	testRedactNode         = "BB9020011AC4202"

	// testRedactCmd is a command carrying cloud credentials.
	testRedactCmd = "backup:namespace=" + testRedactNamespace + ";access-key=" + testRedactAccessVal +
		";secret-key=" + testRedactSensitiveVal + ";s3-bucket=" + testRedactBucket
)

func Test_redactCmd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cmd         string
		wantMissing []string
		wantPresent []string
	}{
		{
			name:        "command with credentials",
			cmd:         testRedactCmd,
			wantMissing: []string{testRedactAccessVal, testRedactSensitiveVal},
			wantPresent: []string{
				"access-key=" + redactedValue,
				"secret-key=" + redactedValue,
				testRedactNamespace,
				testRedactBucket,
			},
		},
		{
			name:        "response echoing the command back",
			cmd:         "ERROR:4:failed for secret-key=" + testRedactSensitiveVal,
			wantMissing: []string{testRedactSensitiveVal},
			wantPresent: []string{"secret-key=" + redactedValue},
		},
		{
			name:        "command without credentials is unchanged",
			cmd:         "statistics",
			wantMissing: nil,
			wantPresent: []string{"statistics"},
		},
		{
			name:        "empty credentials are still redacted",
			cmd:         "backup:access-key=;secret-key=;s3-bucket=" + testRedactBucket,
			wantMissing: nil,
			wantPresent: []string{
				"access-key=" + redactedValue,
				"secret-key=" + redactedValue,
				testRedactBucket,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := redactCmd(tt.cmd)

			for _, missing := range tt.wantMissing {
				require.NotContains(t, got, missing)
			}

			for _, present := range tt.wantPresent {
				require.Contains(t, got, present)
			}
		})
	}

	// Every configured parameter must be covered, so that adding a name to
	// sensitiveParams is all it takes to have its value redacted.
	t.Run("every configured parameter is redacted", func(t *testing.T) {
		t.Parallel()

		require.NotEmpty(t, sensitiveParams)

		for _, param := range sensitiveParams {
			got := redactCmd("cmd:" + param + "=" + testRedactSensitiveVal + ";namespace=" + testRedactNamespace)

			require.NotContains(t, got, testRedactSensitiveVal, param)
			require.Contains(t, got, param+"="+redactedValue, param)
			require.Contains(t, got, "namespace="+testRedactNamespace, param)
		}
	})
}

func Test_parseResultResponse_RedactsCredentials(t *testing.T) {
	t.Parallel()

	cmd := testRedactCmd

	tests := []struct {
		name   string
		result map[string]string
	}{
		{
			name:   "no response for command",
			result: map[string]string{},
		},
		{
			name:   "command failed",
			result: map[string]string{cmd: errCmdRespPrefix + ":4:invalid credentials"},
		},
		{
			name:   "command failed with echoed credentials",
			result: map[string]string{cmd: errCmdRespPrefix + ":4:" + cmd},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseResultResponse(cmd, tt.result)

			require.Error(t, err)
			require.NotContains(t, err.Error(), testRedactSensitiveVal)
			require.NotContains(t, err.Error(), testRedactAccessVal)
		})
	}
}

func Test_requestByNodeName_RedactsCredentials(t *testing.T) {
	t.Parallel()

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().
		GetNodeByName(testRedactNode).
		Return(nil, &a.AerospikeError{ResultCode: atypes.INVALID_NODE_ERROR}).
		Maybe()

	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

	_, err := ic.requestByNodeName(testRedactNode, testRedactCmd)

	require.Error(t, err)
	require.NotContains(t, err.Error(), testRedactSensitiveVal)
	require.NotContains(t, err.Error(), testRedactAccessVal)
	require.Contains(t, err.Error(), "secret-key="+redactedValue)
}
