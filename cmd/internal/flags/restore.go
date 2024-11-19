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

type Restore struct {
	models.Restore
}

func NewRestore() *Restore {
	return &Restore{}
}

func (f *Restore) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.InputFile, "input-file", "i",
		"",
		"Restore from a single backup file. Use - for stdin.\n"+
			"Required, unless --directory or --directory-list is used.\n")
	flagSet.BoolVarP(&f.Uniq, "unique", "u",
		false,
		"Skip records that already exist in the namespace;\n"+
			"Don't touch them.\n")
	flagSet.BoolVarP(&f.Replace, "replace", "r",
		false,
		"Fully replace records that already exist in the namespace;\n"+
			"Don't update them.\n")
	flagSet.BoolVarP(&f.NoGeneration, "no-generation", "g",
		false,
		"Don't check the generation of records that already exist in the namespace.")
	flagSet.BoolVar(&f.IgnoreRecordError, "ignore-record-error",
		false,
		"Ignore permanent record specific error. e.g AEROSPIKE_RECORD_TOO_BIG.\n"+
			"By default such errors are not ignored and asrestore terminates.\n"+
			"Optional: Use verbose mode to see errors in detail.")
	flagSet.BoolVar(&f.DisableBatchWrites, "disable-batch-writes",
		false,
		"Disables the use of batch writes when restoring records to the Aerospike cluster.\n"+
			"By default, the cluster is checked for batch write support, so only set this flag if you explicitly\n"+
			"don't want\nbatch writes to be used or asrestore is failing to recognize that batch writes are disabled\n"+
			"and is failing to work because of it.")
	flagSet.IntVar(&f.MaxAsyncBatches, "max-async-batches",
		32,
		"The max number of outstanding async record batch write calls at a time.\n"+
			"For pre-6.0 servers, 'batches' are only a logical grouping of\n"+
			"records, and each record is uploaded individually. The true max\n"+
			"number of async aerospike calls would then be\n"+
			"<max-async-batches> * <batch-size>.")
	flagSet.IntVar(&f.BatchSize, "batch-size", 128,
		"The max allowed number of records to simultaneously upload\n"+
			"in an async batch write calls to make to aerospike at a time.\n"+
			"Default is 128 with batch writes enabled, or 16 without batch writes.")
	flagSet.Int64Var(&f.ExtraTTL, "extra-ttl",
		0,
		"For records with expirable void-times, add N seconds of extra-ttl to the\n"+
			"recorded void-time.")

	flagSet.Int64VarP(&f.TimeOut, "timeout", "T",
		10000,
		"Set the timeout (ms) for commands.")
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

func (f *Restore) GetRestore() *models.Restore {
	return &f.Restore
}
