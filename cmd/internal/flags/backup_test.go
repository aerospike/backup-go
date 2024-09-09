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

func TestBackup_NewFlagSet(t *testing.T) {
	t.Parallel()
	backup := NewBackup()

	flagSet := backup.NewFlagSet()

	args := []string{
		"--namespace", "test-namespace",
		"--set", "set1", "--set", "set2",
		"--bin-list", "bin1", "--bin-list", "bin2",
		"--parallel", "5",
		"--records-per-second", "100",
		"--file-limit", "5000",
		"--after-digest", "some-digest",
		"--no-records",
		"--modified-before", "2023-09-01_12:00:00",
		"--modified-after", "2023-09-02_12:00:00",
		"--max-retries", "3",
		"--max-records", "1000",
		"--no-bins",
		"--sleep-between-retries", "10",
		"--filter-exp", "encoded-filter-exp",
		"--total-timeout", "20000",
		"--socket-timeout", "15000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := backup.GetBackup()

	// Verify that the flags were correctly parsed
	assert.Equal(t, "test-namespace", result.Namespace, "The namespace flag should be parsed correctly")
	assert.ElementsMatch(t, []string{"set1", "set2"}, result.SetList, "The set flag should be parsed correctly")
	assert.ElementsMatch(t, []string{"bin1", "bin2"}, result.BinList, "The bin-list flag should be parsed correctly")
	assert.Equal(t, 5, result.Parallel, "The parallel flag should be parsed correctly")
	assert.Equal(t, 100, result.RecordsPerSecond, "The records-per-second flag should be parsed correctly")
	assert.Equal(t, int64(5000), result.FileLimit, "The file-limit flag should be parsed correctly")
	assert.Equal(t, "some-digest", result.AfterDigest, "The after-digest flag should be parsed correctly")
	assert.True(t, result.NoRecords, "The no-records flag should be parsed correctly")
	assert.Equal(t, "2023-09-01_12:00:00", result.ModifiedBefore, "The modified-before flag should be parsed correctly")
	assert.Equal(t, "2023-09-02_12:00:00", result.ModifiedAfter, "The modified-after flag should be parsed correctly")
	assert.Equal(t, 3, result.MaxRetries, "The max-retries flag should be parsed correctly")
	assert.Equal(t, int64(1000), result.MaxRecords, "The max-records flag should be parsed correctly")
	assert.True(t, result.NoBins, "The no-bins flag should be parsed correctly")
	assert.Equal(t, 10, result.SleepBetweenRetries, "The sleep-between-retries flag should be parsed correctly")
	assert.Equal(t, "encoded-filter-exp", result.FilterExpression, "The filter-exp flag should be parsed correctly")
	assert.Equal(t, 20000, result.TotalTimeout, "The total-timeout flag should be parsed correctly")
	assert.Equal(t, 15000, result.SocketTimeout, "The socket-timeout flag should be parsed correctly")
}

func TestBackup_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	backup := NewBackup()

	flagSet := backup.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := backup.GetBackup()

	assert.Equal(t, "", result.Namespace, "The default value for namespace should be an empty string")
	assert.Nil(t, result.SetList, "The default value for set should be nil")
	assert.Nil(t, result.BinList, "The default value for bin-list should be nil")
	assert.Equal(t, 1, result.Parallel, "The default value for parallel should be 1")
	assert.Equal(t, 0, result.RecordsPerSecond, "The default value for records-per-second should be 0")
	assert.Equal(t, int64(0), result.FileLimit, "The default value for file-limit should be 0")
	assert.Equal(t, "", result.AfterDigest, "The default value for after-digest should be an empty string")
	assert.False(t, result.NoRecords, "The default value for no-records should be false")
	assert.Equal(t, "", result.ModifiedBefore, "The default value for modified-before should be an empty string")
	assert.Equal(t, "", result.ModifiedAfter, "The default value for modified-after should be an empty string")
	assert.Equal(t, 5, result.MaxRetries, "The default value for max-retries should be 5")
	assert.Equal(t, int64(0), result.MaxRecords, "The default value for max-records should be 0")
	assert.False(t, result.NoBins, "The default value for no-bins should be false")
	assert.Equal(t, 5, result.SleepBetweenRetries, "The default value for sleep-between-retries should be 5")
	assert.Equal(t, "", result.FilterExpression, "The default value for filter-exp should be an empty string")
	assert.Equal(t, 0, result.TotalTimeout, "The default value for total-timeout should be 0")
	assert.Equal(t, 10000, result.SocketTimeout, "The default value for socket-timeout should be 10000")
}
