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

package options

import (
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
)

// mockValidator implements the validator interface for testing
type mockValidator struct{}

func (m *mockValidator) Run(_ string) error {
	return nil
}

func TestWithDir(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithDir("/test/path")(opts)

	assert.Equal(t, []string{"/test/path"}, opts.PathList)
	assert.True(t, opts.IsDir)
}

func TestWithDirList(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	paths := []string{"/test/path1", "/test/path2"}
	WithDirList(paths)(opts)

	assert.Equal(t, paths, opts.PathList)
	assert.True(t, opts.IsDir)
}

func TestWithFile(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithFile("/test/file.txt")(opts)

	assert.Equal(t, []string{"/test/file.txt"}, opts.PathList)
	assert.False(t, opts.IsDir)
}

func TestWithFileList(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	files := []string{"/test/file1.txt", "/test/file2.txt"}
	WithFileList(files)(opts)

	assert.Equal(t, files, opts.PathList)
	assert.False(t, opts.IsDir)
}

func TestWithValidator(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	validator := &mockValidator{}
	WithValidator(validator)(opts)

	assert.Equal(t, validator, opts.Validator)
}

func TestWithNestedDir(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithNestedDir()(opts)

	assert.True(t, opts.WithNestedDir)
}

func TestWithRemoveFiles(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithRemoveFiles()(opts)

	assert.True(t, opts.IsRemovingFiles)
}

func TestWithStartAfter(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithStartAfter("lastfile.txt")(opts)

	assert.Equal(t, "lastfile.txt", opts.StartAfter)
}

func TestWithSkipDirCheck(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithSkipDirCheck()(opts)

	assert.True(t, opts.SkipDirCheck)
}

func TestWithSorting(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithSorting()(opts)

	assert.True(t, opts.SortFiles)
}

func TestWithUploadConcurrency(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithUploadConcurrency(5)(opts)

	assert.Equal(t, 5, opts.UploadConcurrency)
}

func TestWithStorageClass(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithStorageClass("STANDARD")(opts)

	assert.Equal(t, "STANDARD", opts.StorageClass)
}

func TestWithAccessTier(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithAccessTier("Hot")(opts)

	assert.Equal(t, "Hot", opts.AccessTier)
}

func TestWithLogger(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	logger := slog.Default()
	WithLogger(logger)(opts)

	assert.Equal(t, logger, opts.Logger)
}

func TestWithWarmPollDuration(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	duration := 5 * time.Minute
	WithWarmPollDuration(duration)(opts)

	assert.Equal(t, duration, opts.PollWarmDuration)
}

func TestWithChunkSize(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithChunkSize(1024)(opts)

	assert.Equal(t, 1024, opts.ChunkSize)
}

func TestWithChecksum(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	WithChecksum()(opts)

	assert.True(t, opts.WithChecksum)
}

func TestWithRetryPolicy(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	policy := models.NewRetryPolicy(100*time.Millisecond, 2.0, 3)
	WithRetryPolicy(policy)(opts)

	assert.Equal(t, policy, opts.RetryPolicy)
}

func TestCombinedOptions(t *testing.T) {
	t.Parallel()
	opts := &Options{}
	// Apply multiple options
	WithDir("/test/path")(opts)
	WithNestedDir()(opts)
	WithRemoveFiles()(opts)
	WithSorting()(opts)
	WithUploadConcurrency(10)(opts)

	// Verify all options were applied correctly
	assert.Equal(t, []string{"/test/path"}, opts.PathList)
	assert.True(t, opts.IsDir)
	assert.True(t, opts.WithNestedDir)
	assert.True(t, opts.IsRemovingFiles)
	assert.True(t, opts.SortFiles)
	assert.Equal(t, 10, opts.UploadConcurrency)
}

func TestAppendingPaths(t *testing.T) {
	t.Parallel()
	opts := &Options{}

	// Add multiple paths
	WithDir("/test/path1")(opts)
	WithDir("/test/path2")(opts)

	// Verify paths were appended
	assert.Equal(t, []string{"/test/path1", "/test/path2"}, opts.PathList)
	assert.True(t, opts.IsDir)
}

func TestOverridingOptions(t *testing.T) {
	t.Parallel()
	opts := &Options{}

	// Set directory mode
	WithDir("/test/dir")(opts)
	assert.True(t, opts.IsDir)

	// Override with file mode
	WithFile("/test/file.txt")(opts)
	assert.False(t, opts.IsDir)
	assert.Equal(t, []string{"/test/dir", "/test/file.txt"}, opts.PathList)
}
