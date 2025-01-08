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

func TestRestoreXDR_NewFlagSet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want map[string]interface{}
	}{
		{
			name: "all flags set",
			args: []string{
				"--namespace", "test-ns",
				"--input-file", "backup-file.bak",
				"--directory", "test-dir",
				"--parallel", "4",
				"--records-per-second", "1000",
				"--max-retries", "10",
				"--total-timeout", "20000",
				"--socket-timeout", "5000",
				"--ignore-record-error",
				"--timeout", "15000",
				"--retry-base-timeout", "2000",
				"--retry-multiplier", "2.5",
				"--retry-max-retries", "5",
			},
			want: map[string]interface{}{
				"Namespace":         "test-ns",
				"InputFile":         "backup-file.bak",
				"Directory":         "test-dir",
				"Parallel":          4,
				"RecordsPerSecond":  1000,
				"MaxRetries":        10,
				"TotalTimeout":      int64(20000),
				"SocketTimeout":     int64(5000),
				"IgnoreRecordError": true,
				"TimeOut":           int64(15000),
				"RetryBaseTimeout":  int64(2000),
				"RetryMultiplier":   2.5,
				"RetryMaxRetries":   uint(5),
			},
		},
		{
			name: "default values",
			args: []string{},
			want: map[string]interface{}{
				"Namespace":         "",
				"InputFile":         "",
				"Directory":         "",
				"Parallel":          1,
				"RecordsPerSecond":  0,
				"MaxRetries":        5,
				"TotalTimeout":      int64(defaultTotalTimeoutRestore),
				"SocketTimeout":     int64(10000),
				"IgnoreRecordError": false,
				"TimeOut":           int64(10000),
				"RetryBaseTimeout":  int64(1000),
				"RetryMultiplier":   float64(1),
				"RetryMaxRetries":   uint(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			restore := NewRestoreXDR()
			flagSet := restore.NewFlagSet()

			err := flagSet.Parse(tt.args)
			assert.NoError(t, err)

			result := restore.GetRestoreXDR()

			// Verify all fields match expected values
			assert.Equal(t, tt.want["Namespace"], result.Namespace, "Namespace mismatch")
			assert.Equal(t, tt.want["InputFile"], result.InputFile, "InputFile mismatch")
			assert.Equal(t, tt.want["Directory"], result.Directory, "Directory mismatch")
			assert.Equal(t, tt.want["Parallel"], result.Parallel, "Parallel mismatch")
			assert.Equal(t, tt.want["RecordsPerSecond"], result.RecordsPerSecond, "RecordsPerSecond mismatch")
			assert.Equal(t, tt.want["MaxRetries"], result.MaxRetries, "MaxRetries mismatch")
			assert.Equal(t, tt.want["TotalTimeout"], result.TotalTimeout, "TotalTimeout mismatch")
			assert.Equal(t, tt.want["SocketTimeout"], result.SocketTimeout, "SocketTimeout mismatch")
			assert.Equal(t, tt.want["IgnoreRecordError"], result.IgnoreRecordError, "IgnoreRecordError mismatch")
			assert.Equal(t, tt.want["TimeOut"], result.TimeOut, "TimeOut mismatch")
			assert.Equal(t, tt.want["RetryBaseTimeout"], result.RetryBaseTimeout, "RetryBaseTimeout mismatch")
			assert.Equal(t, tt.want["RetryMultiplier"], result.RetryMultiplier, "RetryMultiplier mismatch")
			assert.Equal(t, tt.want["RetryMaxRetries"], result.RetryMaxRetries, "RetryMaxRetries mismatch")
		})
	}
}
