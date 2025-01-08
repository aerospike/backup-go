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

package asbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_Run(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		wantErr  string
	}{
		{
			name:     "valid extension",
			fileName: "backup.asbx",
		},
		{
			name:     "valid extension with path",
			fileName: "/path/to/backup.asbx",
		},
		{
			name:     "valid extension with multiple dots",
			fileName: "backup.2024.01.asbx",
		},
		{
			name:     "invalid extension",
			fileName: "backup.txt",
			wantErr:  "restore file backup.txt is in an invalid format, expected extension: .asbx, got: .txt",
		},
		{
			name:     "no extension",
			fileName: "backup",
			wantErr:  "restore file backup is in an invalid format, expected extension: .asbx, got: ",
		},
		{
			name:     "wrong case extension",
			fileName: "backup.ASBX",
			wantErr:  "restore file backup.ASBX is in an invalid format, expected extension: .asbx, got: .ASBX",
		},
		{
			name:     "dot file without extension",
			fileName: ".backup",
			wantErr:  "restore file .backup is in an invalid format, expected extension: .asbx, got: .backup",
		},
		{
			name:     "empty filename",
			fileName: "",
			wantErr:  "restore file  is in an invalid format, expected extension: .asbx, got: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewValidator()
			err := v.Run(tt.fileName)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				return
			}

			require.NoError(t, err)
		})
	}
}
