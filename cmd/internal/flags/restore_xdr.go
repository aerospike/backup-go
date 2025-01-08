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
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/spf13/pflag"
)

type RestoreXDR struct {
	models.RestoreXDR
}

func NewRestoreXDR() *RestoreXDR {
	return &RestoreXDR{}
}

func (f *RestoreXDR) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		"",
		"Namespace to restore. Example: source-ns")
	flagSet.StringVarP(&f.InputFile, "input-file", "i",
		"",
		"Restore from a single backup file. Use - for stdin.\n"+
			"Required, unless --directory or --directory-list is used.\n")
	flagSet.StringVarP(&f.Directory, "directory", "d",
		"",
		"The Directory that holds the backup files. Required, unless -o or -e is used.")
	flagSet.IntVarP(&f.Parallel, "parallel", "w",
		1,
		descParallelRestore)
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		0,
		"Limit total returned records per second (rps).\n"+
			"Do not apply rps limit if records-per-second is zero.")
	flagSet.IntVar(&f.MaxRetries, "max-retries",
		5,
		"Maximum number of retries before aborting the current transaction.")
	flagSet.Int64Var(&f.TotalTimeout, "total-timeout",
		defaultTotalTimeoutRestore,
		"Total transaction timeout in milliseconds. 0 - no timeout.")
	flagSet.Int64Var(&f.SocketTimeout, "socket-timeout",
		10000,
		"Socket timeout in milliseconds. If this value is 0, it's set to total-timeout. If both are 0,\n"+
			"there is no socket idle time limit")
	flagSet.BoolVar(&f.IgnoreRecordError, "ignore-record-error",
		false,
		"Ignore permanent record specific error. e.g AEROSPIKE_RECORD_TOO_BIG.\n"+
			"By default such errors are not ignored and asrestore terminates.\n"+
			"Optional: Use verbose mode to see errors in detail.")
	flagSet.Int64VarP(&f.TimeOut, "timeout", "T",
		10000,
		"Set the timeout (ms) for info commands.")
	flagSet.Int64Var(&f.RetryBaseTimeout, "retry-base-timeout",
		1000,
		"Set the initial delay between retry attempts in milliseconds")
	flagSet.Float64Var(&f.RetryMultiplier, "retry-multiplier",
		1,
		"retry-multiplier is used to increase the delay between subsequent retry attempts.\n"+
			"The actual delay is calculated as: retry-base-timeout * (retry-multiplier ^ attemptNumber)")
	flagSet.UintVar(&f.RetryMaxRetries, "retry-max-retries",
		0,
		"Set the maximum number of retry attempts that will be made. If set to 0, no retries will be performed.")

	return flagSet
}

func (f *RestoreXDR) GetRestoreXDR() *models.RestoreXDR {
	return &f.RestoreXDR
}
