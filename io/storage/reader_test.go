package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
			ctx := context.Background()
			r := new(mockReader)

			r.On("ListObjects", ctx, "test/path").Return(tt.inputFiles, nil)
			r.On("SetObjectsToStream", tt.sortedFiles).Return()

			err := PreSort(ctx, r, "test/path")

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			r.AssertExpectations(t)
		})
	}
}

// TestCleanPath verifies the CleanPath function's behavior for different input paths
func TestCleanPath(t *testing.T) {
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
			result := CleanPath(tt.path, tt.isS3)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsDirectory verifies the IsDirectory function's behavior for different file paths
func TestIsDirectory(t *testing.T) {
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
			result := IsDirectory(tt.prefix, tt.fileName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
