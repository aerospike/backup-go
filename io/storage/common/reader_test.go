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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockReader implements the reader interface for testing
type mockReader struct {
	mock.Mock
}

func (m *mockReader) ListObjects(ctx context.Context, path string) ([]string, error) {
	args := m.Called(ctx, path)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockReader) SetObjectsToStream(list []string) {
	m.Called(list)
}

// TestPreSort verifies the PreSort function's behavior for sorting backup files
func TestPreSort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		inputFiles  []string
		sortedFiles []string
		expectError bool
	}{
		{
			name:        "empty list",
			inputFiles:  []string{},
			sortedFiles: []string{},
			expectError: false,
		},
		{
			name:        "single file",
			inputFiles:  []string{"0_backup_1.asbx"},
			sortedFiles: []string{"0_backup_1.asbx"},
			expectError: false,
		},
		{
			name:        "multiple files",
			inputFiles:  []string{"0_backup_2.asbx", "0_backup_1.asbx", "0_backup_3.asbx"},
			sortedFiles: []string{"0_backup_1.asbx", "0_backup_2.asbx", "0_backup_3.asbx"},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			r := new(mockReader)

			r.On("ListObjects", ctx, "test/path").Return(tt.inputFiles, nil)
			r.On("SetObjectsToStream", tt.sortedFiles).Return()

			err := PreSort(ctx, r, "test/path")

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			r.AssertExpectations(t)
		})
	}
}

// TestCleanPath verifies the CleanPath function's behavior for different input paths
func TestCleanPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		path     string
		isS3     bool
		expected string
	}{
		{
			name:     "empty path",
			path:     "",
			isS3:     false,
			expected: "",
		},
		{
			name:     "root path for S3",
			path:     "/",
			isS3:     true,
			expected: "",
		},
		{
			name:     "root path for non-S3",
			path:     "/",
			isS3:     false,
			expected: "/",
		},
		{
			name:     "path without trailing slash",
			path:     "/path/to/dir",
			isS3:     false,
			expected: "/path/to/dir/",
		},
		{
			name:     "path with trailing slash",
			path:     "/path/to/dir/",
			isS3:     false,
			expected: "/path/to/dir/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := CleanPath(tt.path, tt.isS3)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsDirectory verifies the IsDirectory function's behavior for different file paths
func TestIsDirectory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		prefix   string
		fileName string
		expected bool
	}{
		{
			name:     "ends with slash",
			prefix:   "backup",
			fileName: "backup/dir/",
			expected: true,
		},
		{
			name:     "inside prefix folder",
			prefix:   "backup",
			fileName: "backup/file.txt",
			expected: false,
		},
		{
			name:     "root file",
			prefix:   "",
			fileName: "file.txt",
			expected: false,
		},
		{
			name:     "nested file in prefix",
			prefix:   "backup/",
			fileName: "backup/dir/file.txt",
			expected: true,
		},
		{
			name:     "file in root prefix",
			prefix:   "/",
			fileName: "/file.txt",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsDirectory(tt.prefix, tt.fileName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
