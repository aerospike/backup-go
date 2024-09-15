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

func TestCommon_NewFlagSet(t *testing.T) {
	t.Parallel()
	common := NewCommon()

	flagSet := common.NewFlagSet()

	args := []string{
		"--directory", "/path/to/backup",
		"--namespace", "test-namespace",
		"--set", "set1",
		"--set", "set2",
		"--records-per-second", "5000",
		"--bin-list", "bin1",
		"--bin-list", "bin2",
		"--parallel", "10",
		"--no-records",
		"--no-indexes",
		"--no-udfs",
		"--max-retries", "3",
		"--total-timeout", "30000",
		"--socket-timeout", "15000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := common.GetCommon()

	assert.Equal(t, "/path/to/backup", result.Directory, "The directory flag should be parsed correctly")
	assert.Equal(t, "test-namespace", result.Namespace, "The namespace flag should be parsed correctly")
	assert.Equal(t, []string{"set1", "set2"}, result.SetList, "The set list flag should be parsed correctly")
	assert.Equal(t, 5000, result.RecordsPerSecond, "The records-per-second flag should be parsed correctly")
	assert.Equal(t, []string{"bin1", "bin2"}, result.BinList, "The bin-list flag should be parsed correctly")
	assert.Equal(t, 10, result.Parallel, "The parallel flag should be parsed correctly")
	assert.True(t, result.NoRecords, "The no-records flag should be parsed correctly")
	assert.True(t, result.NoIndexes, "The no-indexes flag should be parsed correctly")
	assert.True(t, result.NoUDFs, "The no-udfs flag should be parsed correctly")
	assert.Equal(t, 3, result.MaxRetries, "The max-retries flag should be parsed correctly")
	assert.Equal(t, int64(30000), result.TotalTimeout, "The total-timeout flag should be parsed correctly")
	assert.Equal(t, int64(15000), result.SocketTimeout, "The socket-timeout flag should be parsed correctly")
}

func TestCommon_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	common := NewCommon()

	flagSet := common.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := common.GetCommon()

	// Verify default values
	assert.Equal(t, "", result.Directory, "The default value for directory should be an empty string")
	assert.Equal(t, "", result.Namespace, "The default value for namespace should be an empty string")
	assert.Nil(t, result.SetList, "The default value for set-list should be nil")
	assert.Equal(t, 0, result.RecordsPerSecond, "The default value for records-per-second should be 0")
	assert.Nil(t, result.BinList, "The default value for bin-list should be nil")
	assert.Equal(t, 1, result.Parallel, "The default value for parallel should be 1")
	assert.False(t, result.NoRecords, "The default value for no-records should be false")
	assert.False(t, result.NoIndexes, "The default value for no-indexes should be false")
	assert.False(t, result.NoUDFs, "The default value for no-udfs should be false")
	assert.Equal(t, 5, result.MaxRetries, "The default value for max-retries should be 5")
	assert.Equal(t, int64(0), result.TotalTimeout, "The default value for total-timeout should be 0")
	assert.Equal(t, int64(10000), result.SocketTimeout, "The default value for socket-timeout should be 10000")
}
