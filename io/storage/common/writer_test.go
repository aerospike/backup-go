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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFullPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    string
		filename  string
		pathList  []string
		isDir     bool
		isRecords bool
		want      string
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "directory with prefix and filename",
			prefix:    "/backup",
			filename:  "mydir",
			pathList:  nil,
			isDir:     true,
			isRecords: false,
			want:      "/backup/mydir",
			wantErr:   false,
		},
		{
			name:      "directory with empty prefix",
			prefix:    "",
			filename:  "mydir",
			pathList:  nil,
			isDir:     true,
			isRecords: false,
			want:      "mydir",
			wantErr:   false,
		},
		{
			name:      "directory with nested path",
			prefix:    "/backup/2024",
			filename:  "data",
			pathList:  []string{"ignored", "paths"},
			isDir:     true,
			isRecords: false,
			want:      "/backup/2024/data",
			wantErr:   false,
		},
		{
			name:      "metadata file with single path in pathList",
			prefix:    "/backup",
			filename:  "metadata.json",
			pathList:  []string{"data/records.bin"},
			isDir:     false,
			isRecords: false,
			want:      "data/metadata.json",
			wantErr:   false,
		},
		{
			name:      "metadata file with nested path",
			prefix:    "/backup",
			filename:  "metadata.json",
			pathList:  []string{"/data/backup/records.bin"},
			isDir:     false,
			isRecords: false,
			want:      "/data/backup/metadata.json",
			wantErr:   false,
		},
		{
			name:      "metadata file with multiple paths - uses first",
			prefix:    "/backup",
			filename:  "metadata.json",
			pathList:  []string{"data/first.bin", "data/second.bin"},
			isDir:     false,
			isRecords: false,
			want:      "data/metadata.json",
			wantErr:   false,
		},
		{
			name:      "metadata file with path in root",
			prefix:    "/backup",
			filename:  "metadata.json",
			pathList:  []string{"records.bin"},
			isDir:     false,
			isRecords: false,
			want:      "metadata.json",
			wantErr:   false,
		},
		{
			name:      "metadata file with empty pathList - error",
			prefix:    "/backup",
			filename:  "metadata.json",
			pathList:  []string{},
			isDir:     false,
			isRecords: false,
			want:      "",
			wantErr:   true,
			errMsg:    "path list can't be empty",
		},
		{
			name:      "metadata file with nil pathList - error",
			prefix:    "/backup",
			filename:  "metadata.json",
			pathList:  nil,
			isDir:     false,
			isRecords: false,
			want:      "",
			wantErr:   true,
			errMsg:    "path list can't be empty",
		},
		{
			name:      "records file with single path",
			prefix:    "/backup",
			filename:  "ignored",
			pathList:  []string{"data.bin"},
			isDir:     false,
			isRecords: true,
			want:      "/backup/data.bin",
			wantErr:   false,
		},
		{
			name:      "records file with nested path",
			prefix:    "/backup/2024",
			filename:  "ignored",
			pathList:  []string{"records/file.bin"},
			isDir:     false,
			isRecords: true,
			want:      "/backup/2024/records/file.bin",
			wantErr:   false,
		},
		{
			name:      "records file with multiple paths - uses first",
			prefix:    "/backup",
			filename:  "ignored",
			pathList:  []string{"first.bin", "second.bin", "third.bin"},
			isDir:     false,
			isRecords: true,
			want:      "/backup/first.bin",
			wantErr:   false,
		},
		{
			name:      "records file with empty prefix",
			prefix:    "",
			filename:  "ignored",
			pathList:  []string{"data.bin"},
			isDir:     false,
			isRecords: true,
			want:      "data.bin",
			wantErr:   false,
		},
		{
			name:      "records file with empty pathList - error",
			prefix:    "/backup",
			filename:  "ignored",
			pathList:  []string{},
			isDir:     false,
			isRecords: true,
			want:      "",
			wantErr:   true,
			errMsg:    "path list can't be empty",
		},
		{
			name:      "records file with nil pathList - error",
			prefix:    "/backup",
			filename:  "ignored",
			pathList:  nil,
			isDir:     false,
			isRecords: true,
			want:      "",
			wantErr:   true,
			errMsg:    "path list can't be empty",
		},
		{
			name:      "edge case - all empty strings for directory",
			prefix:    "",
			filename:  "",
			pathList:  nil,
			isDir:     true,
			isRecords: false,
			want:      "",
			wantErr:   false,
		},
		{
			name:      "edge case - special characters in paths",
			prefix:    "/backup/special-chars_123",
			filename:  "file-name_v2.json",
			pathList:  []string{"data/special_file-123.bin"},
			isDir:     false,
			isRecords: false,
			want:      "data/file-name_v2.json",
			wantErr:   false,
		},
		{
			name:      "edge case - dot paths",
			prefix:    "./backup",
			filename:  "data",
			pathList:  []string{"./records/file.bin"},
			isDir:     false,
			isRecords: true,
			want:      "backup/records/file.bin",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := GetFullPath(tt.prefix, tt.filename, tt.pathList, tt.isDir, tt.isRecords)

			if tt.wantErr {
				require.Error(t, err, "expected error but got none")
				assert.Equal(t, tt.errMsg, err.Error(), "error message mismatch")
				assert.Equal(t, tt.want, got, "expected empty string on error")
				return
			}

			require.NoError(t, err, "unexpected error")
			assert.Equal(t, tt.want, got, "path mismatch")
		})
	}
}

func TestGetFullPath_Consistency(t *testing.T) {
	t.Parallel()

	prefix := "/backup"
	filename := "test.json"
	pathList := []string{"data/records.bin"}

	result1, err1 := GetFullPath(prefix, filename, pathList, false, false)
	result2, err2 := GetFullPath(prefix, filename, pathList, false, false)

	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, result1, result2, "function should be deterministic")
}
