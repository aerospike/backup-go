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

	"github.com/aerospike/backup-go/errclass"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared by the infoCmd tests below.
const (
	testCmdName      = "cmd"
	testCmdKey1      = "k1"
	testCmdKey2      = "k2"
	testCmdValue1    = "v1"
	testCmdWithParam = "cmd:k1=v1"

	// testUnsafeValue would inject an extra parameter if it were sent as is.
	testUnsafeValue = "v1;fuzzy-restore=true"
	// testNewlineValue would inject an extra command if it were sent as is.
	testNewlineValue = "v1\nbackup-abort:job-id=1"
)

func TestInfoCmd_BuildParams(t *testing.T) {
	t.Parallel()

	const (
		testKey3   = "k3"
		testValue2 = "v2"
		testValue3 = "v3"
		wantEmpty  = "cmd:"
	)

	tests := []struct {
		name  string
		build func(c *infoCmd) *infoCmd
		want  string
	}{
		{
			name:  "no parameters",
			build: func(c *infoCmd) *infoCmd { return c },
			want:  wantEmpty,
		},
		{
			name:  "single parameter",
			build: func(c *infoCmd) *infoCmd { return c.str(testCmdKey1, testCmdValue1) },
			want:  testCmdWithParam,
		},
		{
			name: "several parameters keep call order without trailing separator",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testKey3, testValue3).str(testCmdKey1, testCmdValue1).str(testCmdKey2, testValue2)
			},
			want: "cmd:k3=v3;k1=v1;k2=v2",
		},
		{
			name: "empty first parameter is skipped",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, "").str(testCmdKey2, testValue2).str(testKey3, testValue3)
			},
			want: "cmd:k2=v2;k3=v3",
		},
		{
			name: "empty middle parameter is skipped",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, testCmdValue1).str(testCmdKey2, "").str(testKey3, testValue3)
			},
			want: "cmd:k1=v1;k3=v3",
		},
		{
			name: "empty last parameter is skipped",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, testCmdValue1).str(testCmdKey2, testValue2).str(testKey3, "")
			},
			want: "cmd:k1=v1;k2=v2",
		},
		{
			name: "all parameters empty",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, "").str(testCmdKey2, "").str(testKey3, "")
			},
			want: wantEmpty,
		},
		{
			name:  "flag true",
			build: func(c *infoCmd) *infoCmd { return c.flag(testCmdKey1, true) },
			want:  "cmd:k1=true",
		},
		{
			name:  "flag false is sent",
			build: func(c *infoCmd) *infoCmd { return c.flag(testCmdKey1, false) },
			want:  "cmd:k1=false",
		},
		{
			name:  "num",
			build: func(c *infoCmd) *infoCmd { return c.num(testCmdKey1, 42) },
			want:  "cmd:k1=42",
		},
		{
			name:  "num zero is sent",
			build: func(c *infoCmd) *infoCmd { return c.num(testCmdKey1, 0) },
			want:  "cmd:k1=0",
		},
		{
			name: "mixed parameter kinds",
			build: func(c *infoCmd) *infoCmd {
				return c.num(testCmdKey1, 3).str(testCmdKey2, testValue2).flag(testKey3, false)
			},
			want: "cmd:k1=3;k2=v2;k3=false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.build(newInfoCmd(testCmdName)).build()

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCmd_Build(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		build       func(c *infoCmd) *infoCmd
		want        string
		wantErr     error
		wantErrText string
	}{
		{
			name: "required parameters set",
			build: func(c *infoCmd) *infoCmd {
				return c.required(testCmdKey1, testCmdValue1).str(testCmdKey2, "")
			},
			want: testCmdWithParam,
		},
		{
			name: "required parameter missing",
			build: func(c *infoCmd) *infoCmd {
				return c.required(testCmdKey1, "").str(testCmdKey2, testCmdValue1)
			},
			wantErr:     errMissingCmdParam,
			wantErrText: "cmd: k1",
		},
		{
			name: "all missing parameters are reported",
			build: func(c *infoCmd) *infoCmd {
				return c.required(testCmdKey1, "").required(testCmdKey2, "")
			},
			wantErr:     errMissingCmdParam,
			wantErrText: "cmd: k1, k2",
		},
		{
			name: "separator in optional value",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, testCmdValue1).str(testCmdKey2, testUnsafeValue)
			},
			wantErr:     errInvalidCmdParam,
			wantErrText: "cmd: k2",
		},
		{
			name: "newline in optional value",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, testCmdValue1).str(testCmdKey2, testNewlineValue)
			},
			wantErr:     errInvalidCmdParam,
			wantErrText: "cmd: k2",
		},
		{
			name: "separator in required value",
			build: func(c *infoCmd) *infoCmd {
				return c.required(testCmdKey1, testUnsafeValue)
			},
			wantErr:     errInvalidCmdParam,
			wantErrText: "cmd: k1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.build(newInfoCmd(testCmdName)).build()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				require.ErrorContains(t, err, tt.wantErrText)
				assert.NotContains(t, err.Error(), testUnsafeValue)
				assert.NotContains(t, err.Error(), testNewlineValue)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCmd_BuildMissingAndInvalid(t *testing.T) {
	t.Parallel()

	got, err := newInfoCmd(testCmdName).required(testCmdKey1, "").str(testCmdKey2, testUnsafeValue).build()

	require.ErrorIs(t, err, errMissingCmdParam)
	require.ErrorIs(t, err, errInvalidCmdParam)
	require.ErrorIs(t, err, errclass.ErrInvalidConfig)
	require.ErrorContains(t, err, "cmd: k1")
	require.ErrorContains(t, err, "cmd: k2")
	assert.NotContains(t, err.Error(), testUnsafeValue)
	assert.NotContains(t, err.Error(), "\n", "error must be a single line")
	assert.Empty(t, got)
}

func TestBuildPathCmd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		giveValue string
		want      string
		wantErr   error
	}{
		{
			name:      "value set",
			giveValue: testCmdValue1,
			want:      "cmd/v1",
		},
		{
			name:    "value missing",
			wantErr: errMissingCmdParam,
		},
		{
			name:      "separator in value",
			giveValue: testUnsafeValue,
			wantErr:   errInvalidCmdParam,
		},
		{
			name:      "newline in value",
			giveValue: testNewlineValue,
			wantErr:   errInvalidCmdParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := buildPathCmd(testCmdName, testCmdKey1, tt.giveValue)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				require.ErrorContains(t, err, "cmd: k1")
				assert.NotContains(t, err.Error(), testUnsafeValue)
				assert.NotContains(t, err.Error(), testNewlineValue)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
