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

package flags

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRestore_NewFlagSet(t *testing.T) {
	t.Parallel()
	restore := NewRestore()

	flagSet := restore.NewFlagSet()

	args := []string{
		"--input-file", "backup-file.bak",
		"--ignore-record-error",
		"--disable-batch-writes",
		"--max-async-batches", "64",
		"--batch-size", "256",
		"--extra-ttl", "3600",
		"--directory-list", "dir1,dir2",
		"--parent-directory", "parent-dir",
		"--warm-up", "10",
		"--validate", "true",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := restore.GetRestore()

	assert.Equal(t, "backup-file.bak", result.InputFile, "The input-file flag should be parsed correctly")
	assert.True(t, result.IgnoreRecordError, "The ignore-record-error flag should be parsed correctly")
	assert.True(t, result.DisableBatchWrites, "The disable-batch-writes flag should be parsed correctly")
	assert.Equal(t, 64, result.MaxAsyncBatches, "The max-async-batches flag should be parsed correctly")
	assert.Equal(t, 256, result.BatchSize, "The batch-size flag should be parsed correctly")
	assert.Equal(t, int64(3600), result.ExtraTTL, "The extra-ttl flag should be parsed correctly")
	assert.Equal(t, "dir1,dir2", result.DirectoryList, "The directory-list flag should be parsed correctly")
	assert.Equal(t, "parent-dir", result.ParentDirectory, "The parent-directory flag should be parsed correctly")
	assert.Equal(t, 10, result.WarmUp, "The warm-up flag should be parsed correctly")
	assert.Equal(t, true, result.ValidateOnly, "The validate flag should be parsed correctly")
}

func TestRestore_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	restore := NewRestore()

	flagSet := restore.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := restore.GetRestore()

	// Verify default values
	assert.Equal(t, "", result.InputFile, "The default value for input-file should be an empty string")
	assert.False(t, result.IgnoreRecordError, "The default value for ignore-record-error should be false")
	assert.False(t, result.DisableBatchWrites, "The default value for disable-batch-writes should be false")
	assert.Equal(t, 32, result.MaxAsyncBatches, "The default value for max-async-batches should be 32")
	assert.Equal(t, 128, result.BatchSize, "The default value for batch-size should be 128")
	assert.Equal(t, int64(0), result.ExtraTTL, "The default value for extra-ttl should be 0")
	assert.Equal(t, "", result.DirectoryList, "The directory-list flag should be an empty string")
	assert.Equal(t, "", result.ParentDirectory, "The parent-directory flag should be an empty string")
	assert.Equal(t, 0, result.WarmUp, "The warm-up flag should be 0")
	assert.Equal(t, false, result.ValidateOnly, "The validate flag should be false")
}
