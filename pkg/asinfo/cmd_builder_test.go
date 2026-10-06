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

	// testErrKey1 and testErrKey2 are how the errors report the keys k1 and k2.
	testErrKey1 = "cmd: k1"
	testErrKey2 = "cmd: k2"

	// testUnsafeValue would inject an extra parameter if it were sent as is.
	testUnsafeValue = "v1;fuzzy-restore=true"
	// testNewlineValue would inject an extra command if it were sent as is.
	testNewlineValue = "v1\nbackup-abort:job-id=1"
	// testPipeValue is valid unless the command forbids "|".
	testPipeValue = "v1|v2"
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
			name:  "optional flag unset is omitted",
			build: func(c *infoCmd) *infoCmd { return c.optFlag(testCmdKey1, nil) },
			want:  wantEmpty,
		},
		{
			name:  "optional flag false is sent",
			build: func(c *infoCmd) *infoCmd { return c.optFlag(testCmdKey1, testPtr(false)) },
			want:  "cmd:k1=false",
		},
		{
			name:  "optional flag true is sent",
			build: func(c *infoCmd) *infoCmd { return c.optFlag(testCmdKey1, testPtr(true)) },
			want:  "cmd:k1=true",
		},
		{
			name:  "optional num zero is omitted",
			build: func(c *infoCmd) *infoCmd { return c.optNum(testCmdKey1, 0) },
			want:  wantEmpty,
		},
		{
			name:  "optional num is sent",
			build: func(c *infoCmd) *infoCmd { return c.optNum(testCmdKey1, 42) },
			want:  "cmd:k1=42",
		},
		{
			name:  "optional float zero is omitted",
			build: func(c *infoCmd) *infoCmd { return c.optFloat(testCmdKey1, 0) },
			want:  wantEmpty,
		},
		{
			name:  "optional float with fraction",
			build: func(c *infoCmd) *infoCmd { return c.optFloat(testCmdKey1, 1.25) },
			want:  "cmd:k1=1.25",
		},
		{
			name:  "optional float without fraction",
			build: func(c *infoCmd) *infoCmd { return c.optFloat(testCmdKey1, 2) },
			want:  "cmd:k1=2",
		},
		{
			name:  "pipe is allowed by default",
			build: func(c *infoCmd) *infoCmd { return c.str(testCmdKey1, testPipeValue) },
			want:  "cmd:k1=v1|v2",
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
			wantErrText: testErrKey1,
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
			wantErrText: testErrKey2,
		},
		{
			name: "newline in optional value",
			build: func(c *infoCmd) *infoCmd {
				return c.str(testCmdKey1, testCmdValue1).str(testCmdKey2, testNewlineValue)
			},
			wantErr:     errInvalidCmdParam,
			wantErrText: testErrKey2,
		},
		{
			name: "separator in required value",
			build: func(c *infoCmd) *infoCmd {
				return c.required(testCmdKey1, testUnsafeValue)
			},
			wantErr:     errInvalidCmdParam,
			wantErrText: testErrKey1,
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

func TestInfoCmd_BuildForbidden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		giveValue string
		wantErr   error
	}{
		{
			name:      "extra forbidden char",
			giveValue: testPipeValue,
			wantErr:   errInvalidCmdParam,
		},
		{
			name:      "default forbidden char is kept",
			giveValue: testUnsafeValue,
			wantErr:   errInvalidCmdParam,
		},
		{
			name:      "allowed value",
			giveValue: testCmdValue1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newInfoCmd(testCmdName, "|").str(testCmdKey1, tt.giveValue).build()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorContains(t, err, testErrKey1)
				assert.NotContains(t, err.Error(), tt.giveValue)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCmdWithParam, got)
		})
	}
}

func TestInfoCmd_BuildMissingAndInvalid(t *testing.T) {
	t.Parallel()

	got, err := newInfoCmd(testCmdName).required(testCmdKey1, "").str(testCmdKey2, testUnsafeValue).build()

	require.ErrorIs(t, err, errMissingCmdParam)
	require.ErrorIs(t, err, errInvalidCmdParam)
	require.ErrorIs(t, err, errclass.ErrInvalidConfig)
	require.ErrorContains(t, err, testErrKey1)
	require.ErrorContains(t, err, testErrKey2)
	assert.NotContains(t, err.Error(), testUnsafeValue)
	assert.NotContains(t, err.Error(), "\n", "error must be a single line")
	assert.Empty(t, got)
}

func TestCommaSeparatedListValid(t *testing.T) {
	t.Parallel()

	const (
		testListSingleID  = "260901T000000-abcd"
		testListMultiple  = "260901T000000-abcd,260901T000001-efgh"
		testListCommaOnly = ","
		testListEmptyPart = "a,,b"
		testListLeading   = ",a"
		testListTrailing  = "a,"
	)

	tests := []struct {
		name  string
		give  string
		valid bool
	}{
		{name: "single id", give: testListSingleID, valid: true},
		{name: "multiple ids", give: testListMultiple, valid: true},
		{name: "comma only", give: testListCommaOnly, valid: false},
		{name: "empty entry", give: testListEmptyPart, valid: false},
		{name: "leading comma", give: testListLeading, valid: false},
		{name: "trailing comma", give: testListTrailing, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, commaSeparatedListValid(tt.give))
		})
	}
}

func TestInfoCmd_RequiredCommaList(t *testing.T) {
	t.Parallel()

	const (
		testListValue      = "id1,id2"
		testListWant       = "cmd:k1=id1,id2"
		testListInvalid    = "id1,"
		testListInvalidKey = "cmd: k1"
	)

	tests := []struct {
		name        string
		giveValue   string
		want        string
		wantErr     error
		wantErrText string
	}{
		{
			name:      "valid list",
			giveValue: testListValue,
			want:      testListWant,
		},
		{
			name:        "missing value",
			giveValue:   "",
			wantErr:     errMissingCmdParam,
			wantErrText: testErrKey1,
		},
		{
			name:        "invalid list",
			giveValue:   testListInvalid,
			wantErr:     errInvalidCmdParam,
			wantErrText: testListInvalidKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newInfoCmd(testCmdName).requiredCommaList(testCmdKey1, tt.giveValue).build()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				require.ErrorContains(t, err, tt.wantErrText)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
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
				require.ErrorContains(t, err, testErrKey1)
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
