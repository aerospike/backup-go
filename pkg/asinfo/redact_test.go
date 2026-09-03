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
	"fmt"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo/mocks"
	iModels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/require"
)

const (
	// Fake credential values, only used to assert they never reach logs or errors.
	testRedactAccessVal    = "access-value-fake-0123456789"
	testRedactSensitiveVal = "sensitive-value-fake-9876543210"
	testRedactJobID        = "1700000000"
	testRedactNamespace    = "source-ns"
	testRedactBucket       = "backup-bucket"
	testRedactRegion       = "eu-central-1"
	testRedactProfile      = "default"
	testRedactEndpoint     = "https://s3.example.com"
	testRedactStorage      = "aws-s3"
	testRedactNode         = "BB9020011AC4202"
)

// testBackupCmd builds a real server backup command carrying cloud credentials.
func testBackupCmd() string {
	return fmt.Sprintf(cmdServerBackup,
		testRedactNamespace,
		testRedactJobID,
		testRedactStorage,
		testRedactBucket,
		testRedactRegion,
		testRedactProfile,
		testRedactAccessVal,
		testRedactSensitiveVal,
		testRedactEndpoint,
		"", "", "",
		false, false, false,
	)
}

// testRestoreCmd builds a real server restore command carrying cloud credentials.
func testRestoreCmd() string {
	return fmt.Sprintf(cmdServerRestore,
		testRedactNamespace,
		testRedactJobID,
		testRedactStorage,
		testRedactBucket,
		testRedactRegion,
		testRedactProfile,
		testRedactAccessVal,
		testRedactSensitiveVal,
		testRedactEndpoint,
		false,
		"",
	)
}

// testRequestCommon returns request fields carrying cloud credentials.
func testRequestCommon() iModels.RequestCommon {
	return iModels.RequestCommon{
		Namespace: testRedactNamespace,
		Storage:   testRedactStorage,
		Bucket:    testRedactBucket,
		Region:    testRedactRegion,
		Profile:   testRedactProfile,
		AccessKey: testRedactAccessVal,
		SecretKey: testRedactSensitiveVal,
		Endpoint:  testRedactEndpoint,
	}
}

func Test_redactCmd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cmd         string
		wantMissing []string
		wantPresent []string
	}{
		{
			name:        "server backup command",
			cmd:         testBackupCmd(),
			wantMissing: []string{testRedactAccessVal, testRedactSensitiveVal},
			wantPresent: []string{
				"access-key=" + redactedValue,
				"secret-key=" + redactedValue,
				testRedactNamespace,
				testRedactBucket,
			},
		},
		{
			name:        "server restore command",
			cmd:         testRestoreCmd(),
			wantMissing: []string{testRedactAccessVal, testRedactSensitiveVal},
			wantPresent: []string{
				"access-key=" + redactedValue,
				"secret-key=" + redactedValue,
				testRedactEndpoint,
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

	cmd := testBackupCmd()

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

func Test_getByNode_RedactsCredentials(t *testing.T) {
	t.Parallel()

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().
		GetNodeByName(testRedactNode).
		Return(nil, &a.AerospikeError{ResultCode: atypes.INVALID_NODE_ERROR}).
		Maybe()

	ic := newClient(mockNodeGetter, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	_, err := ic.getByNode(testRedactNode, testBackupCmd())

	require.Error(t, err)
	require.NotContains(t, err.Error(), testRedactSensitiveVal)
	require.NotContains(t, err.Error(), testRedactAccessVal)
	require.Contains(t, err.Error(), "secret-key="+redactedValue)
}

// Test_StartServer_DoesNotLeakSecretKey is the acceptance check: the full
// secret key must never reach slog records, neither on the default logger nor
// at Info level, and it must not surface in the returned error either.
func Test_StartServer_DoesNotLeakSecretKey(t *testing.T) {
	// Not parallel: the test swaps the default logger.
	var (
		defaultBuf bytes.Buffer
		infoBuf    bytes.Buffer
		debugBuf   bytes.Buffer
	)

	slog.SetDefault(slog.New(slog.NewTextHandler(&defaultBuf, nil)))

	common := testRequestCommon()

	tests := []struct {
		name   string
		logger *slog.Logger
		buf    *bytes.Buffer
		run    func(ic *Client) error
	}{
		{
			name:   "backup on default logger",
			logger: slog.Default(),
			buf:    &defaultBuf,
			run: func(ic *Client) error {
				_, err := ic.StartServerBackup(t.Context(), &iModels.RequestBackup{RequestCommon: common})
				return err
			},
		},
		{
			name:   "backup at info level",
			logger: slog.New(slog.NewTextHandler(&infoBuf, &slog.HandlerOptions{Level: slog.LevelInfo})),
			buf:    &infoBuf,
			run: func(ic *Client) error {
				_, err := ic.StartServerBackup(t.Context(), &iModels.RequestBackup{RequestCommon: common})
				return err
			},
		},
		{
			name:   "restore at debug level",
			logger: slog.New(slog.NewTextHandler(&debugBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
			buf:    &debugBuf,
			run: func(ic *Client) error {
				return ic.StartServerRestore(t.Context(), &iModels.RequestRestore{
					RequestCommon: common,
					JobID:         testRedactJobID,
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNodeGetter := mocks.NewMockNodeGetter(t)
			mockNodeGetter.EXPECT().
				GetRandomNode().
				Return(nil, &a.AerospikeError{ResultCode: atypes.INVALID_NODE_ERROR}).
				Maybe()

			ic := newClient(mockNodeGetter, a.NewInfoPolicy(), models.NewRetryPolicy(0, 1, 1))
			ic.logger = tt.logger

			err := tt.run(ic)

			require.Error(t, err)
			require.NotContains(t, err.Error(), testRedactSensitiveVal)
			require.NotContains(t, err.Error(), testRedactAccessVal)
			require.NotContains(t, tt.buf.String(), testRedactSensitiveVal)
			require.NotContains(t, tt.buf.String(), testRedactAccessVal)
		})
	}
}
