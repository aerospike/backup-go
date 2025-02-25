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

func TestBackupXDR_NewFlagSet(t *testing.T) {
	t.Parallel()
	backupXDR := NewBackupXDR()

	flagSet := backupXDR.NewFlagSet()

	args := []string{
		"--namespace", "test-ns",
		"--directory", "/backup/dir",
		"--file-limit", "5000",
		"--parallel-write", "8",
		"--dc", "dc1",
		"--local-address", "192.168.1.1",
		"--local-port", "3000",
		"--rewind", "3600",
		"--read-timeout", "2000",
		"--write-timeout", "2000",
		"--results-queue-size", "512",
		"--ack-queue-size", "512",
		"--max-connections", "200",
		"--info-poling-period", "2000",
		"--stop-xdr",
		"--max-throughput", "1000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := backupXDR.GetBackupXDR()

	assert.Equal(t, "test-ns", result.Namespace, "The namespace flag should be parsed correctly")
	assert.Equal(t, "/backup/dir", result.Directory, "The directory flag should be parsed correctly")
	assert.Equal(t, int64(5000), result.FileLimit, "The file-limit flag should be parsed correctly")
	assert.Equal(t, 8, result.ParallelWrite, "The parallel-write flag should be parsed correctly")
	assert.Equal(t, "dc1", result.DC, "The dc flag should be parsed correctly")
	assert.Equal(t, "192.168.1.1", result.LocalAddress, "The local-address flag should be parsed correctly")
	assert.Equal(t, 3000, result.LocalPort, "The local-port flag should be parsed correctly")
	assert.Equal(t, "3600", result.Rewind, "The rewind flag should be parsed correctly")
	assert.Equal(t, int64(2000), result.ReadTimeoutMilliseconds, "The read-timeout flag should be parsed correctly")
	assert.Equal(t, int64(2000), result.WriteTimeoutMilliseconds, "The write-timeout flag should be parsed correctly")
	assert.Equal(t, 512, result.ResultQueueSize, "The results-queue-size flag should be parsed correctly")
	assert.Equal(t, 512, result.AckQueueSize, "The ack-queue-size flag should be parsed correctly")
	assert.Equal(t, 200, result.MaxConnections, "The max-connections flag should be parsed correctly")
	assert.Equal(t, int64(2000), result.InfoPolingPeriodMilliseconds, "The info-poling-period flag should be parsed correctly")
	assert.Equal(t, true, result.StopXDR, "The stop flag should be parsed correctly")
	assert.Equal(t, 1000, result.MaxThroughput, "The max-throughput flag should be parsed correctly")
}

func TestBackupXDR_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	backupXDR := NewBackupXDR()

	flagSet := backupXDR.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := backupXDR.GetBackupXDR()

	assert.Equal(t, "", result.Namespace, "The default value for namespace should be an empty string")
	assert.Equal(t, "", result.Directory, "The default value for directory should be an empty string")
	assert.Equal(t, int64(262144000), result.FileLimit, "The default value for file-limit should be 250MB")
	assert.Equal(t, 0, result.ParallelWrite, "The default value for parallel-write should be 0")
	assert.Equal(t, "dc", result.DC, "The default value for dc should be 'dc'")
	assert.Equal(t, "127.0.0.1", result.LocalAddress, "The default value for local-address should be '127.0.0.1'")
	assert.Equal(t, 8080, result.LocalPort, "The default value for local-port should be 8080")
	assert.Equal(t, "all", result.Rewind, "The default value for rewind should be 'all'")
	assert.Equal(t, int64(1000), result.ReadTimeoutMilliseconds, "The default value for read-timeout should be 1000")
	assert.Equal(t, int64(1000), result.WriteTimeoutMilliseconds, "The default value for write-timeout should be 1000")
	assert.Equal(t, 256, result.ResultQueueSize, "The default value for results-queue-size should be 256")
	assert.Equal(t, 256, result.AckQueueSize, "The default value for ack-queue-size should be 256")
	assert.Equal(t, 256, result.MaxConnections, "The default value for max-connections should be 256")
	assert.Equal(t, int64(1000), result.InfoPolingPeriodMilliseconds, "The default value for info-poling-period should be 1000")
	assert.Equal(t, 0, result.MaxThroughput, "The default value for max-throughput should be 0")
}

func TestBackupXDR_NewFlagSet_ShortFlags(t *testing.T) {
	t.Parallel()
	backupXDR := NewBackupXDR()

	flagSet := backupXDR.NewFlagSet()

	args := []string{
		"-n", "test-ns",
		"-d", "/backup/dir",
		"-F", "5000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := backupXDR.GetBackupXDR()

	assert.Equal(t, "test-ns", result.Namespace, "The namespace short flag should be parsed correctly")
	assert.Equal(t, "/backup/dir", result.Directory, "The directory short flag should be parsed correctly")
	assert.Equal(t, int64(5000), result.FileLimit, "The file-limit short flag should be parsed correctly")
}

func TestBackupXDR_NewFlagSet_InvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "Invalid parallel-write",
			args: []string{"--parallel-write", "-1"},
		},
		{
			name: "Invalid local-port",
			args: []string{"--local-port", "999999"},
		},
		{
			name: "Invalid queue size",
			args: []string{"--results-queue-size", "-1"},
		},
		{
			name: "Invalid max connections",
			args: []string{"--max-connections", "-1"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backupXDR := NewBackupXDR()
			flagSet := backupXDR.NewFlagSet()
			err := flagSet.Parse(tt.args)
			assert.NoError(t, err, "Flag parsing should succeed even with invalid values")
		})
	}
}
