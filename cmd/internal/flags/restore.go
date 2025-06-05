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
			"Required, unless --directory or --directory-list is used.\n"+
			"Incompatible with --mode=asbx.")
	flagSet.StringVar(&f.DirectoryList, "directory-list",
		"",
		"A comma-separated list of paths to directories that hold the backup files. Required,\n"+
			"unless -i or -d is used. The paths may not contain commas.\n"+
			"Example: 'asrestore --directory-list /path/to/dir1/,/path/to/dir2'\n"+
			"Incompatible with --mode=asbx.")
	flagSet.StringVar(&f.ParentDirectory, "parent-directory",
		"",
		"A common root path for all paths used in --directory-list.\n"+
			"This path is prepended to all entries in --directory-list.\n"+
			"Example: 'asrestore --parent-directory /common/root/path\n"+
			"--directory-list /path/to/dir1/,/path/to/dir2'\n"+
			"Incompatible with --mode=asbx.")
	flagSet.BoolVarP(&f.Uniq, "unique", "u",
		false,
		"Skip modifying records that already exist in the namespace.")
	flagSet.BoolVarP(&f.Replace, "replace", "r",
		false,
		"Fully replace records that already exist in the namespace.\n"+
			"This option still performs a generation check by default and needs to be combined with the -g option\n"+
			"if you do not want to perform a generation check.\n"+
			"This option is mutually exclusive with --unique.")
	flagSet.BoolVarP(&f.NoGeneration, "no-generation", "g",
		false,
		"Don't check the generation of records that already exist in the namespace.")
	flagSet.BoolVar(&f.IgnoreRecordError, "ignore-record-error",
		false,
		"Ignore errors specific to records, not UDFs or indexes. The errors are:\n"+
			"AEROSPIKE_RECORD_TOO_BIG,\n"+
			"AEROSPIKE_KEY_MISMATCH,\n"+
			"AEROSPIKE_BIN_NAME_TOO_LONG,\n"+
			"AEROSPIKE_ALWAYS_FORBIDDEN,\n"+
			"AEROSPIKE_FAIL_FORBIDDEN,\n"+
			"AEROSPIKE_BIN_TYPE_ERROR,\n"+
			"AEROSPIKE_BIN_NOT_FOUND.\n"+
			"By default, these errors are not ignored and asrestore terminates.")
	flagSet.BoolVar(&f.DisableBatchWrites, "disable-batch-writes",
		false,
		"Disables the use of batch writes when restoring records to the Aerospike cluster.\n"+
			"By default, the cluster is checked for batch write support. Only set this flag if you explicitly\n"+
			"don't want batch writes to be used or if asrestore is failing to work because it cannot recognize\n"+
			"that batch writes are disabled.\n"+
			"Incompatible with --mode=asbx.")
	flagSet.IntVar(&f.MaxAsyncBatches, "max-async-batches",
		32,
		"To send data to Aerospike Database, asrestore creates write workers that work in parallel.\n"+
			"This value is the number of workers that form batches and send them to the database.\n"+
			"For Aerospike Database versions prior to 6.0, 'batches' are only a logical grouping of records,\n"+
			"and each record is uploaded individually.\n"+
			"The true max number of async Aerospike calls would then be <max-async-batches> * <batch-size>.\n"+
			"Incompatible with --mode=asbx.")
	flagSet.IntVar(&f.WarmUp, "warm-up",
		0,
		"Warm Up fills the connection pool with connections for all nodes. This is necessary for batch restore.\n"+
			"By default is calculated as (--max-async-batches + 1), as one connection per node is reserved\n"+
			"for tend operations and is not used for transactions.\n"+
			"Incompatible with --mode=asbx.")
	flagSet.IntVar(&f.BatchSize, "batch-size", 128,
		"The max allowed number of records to simultaneously upload to Aerospike.\n"+
			"Default is 128 with batch writes enabled. If you disable batch writes,\n"+
			"this flag is superseded because each worker sends writes one by one.\n"+
			"All three batch flags are linked. If --disable-batch-writes=false,\n"+
			"asrestore uses batch write workers to send data to the database.\n"+
			"Asrestore creates a number of workers equal to --max-async-batches that work in parallel,\n"+
			"and form and send a number of records equal to --batch-size to the database.\n"+
			"Incompatible with --mode=asbx.")
	flagSet.Int64Var(&f.ExtraTTL, "extra-ttl",
		0,
		"For records with expirable void-times, add N seconds of extra-ttl to the\n"+
			"recorded void-time.\n"+
			"Incompatible with --mode=asbx.")
	flagSet.Int64VarP(&f.TimeOut, "timeout", "T",
		10000,
		"Set the timeout (ms) for asinfo commands sent from asrestore to the database.\n"+
			"The info commands are to check version, get indexes, get udfs, count records, and check batch write support.")
	flagSet.Int64Var(&f.RetryBaseTimeout, "retry-base-timeout",
		1000,
		"Set the initial timeout for a retry in milliseconds when data is sent to the Aerospike database\n"+
			"during a restore. This retry sequence is triggered by the following non-critical errors:\n"+
			"AEROSPIKE_NO_AVAILABLE_CONNECTIONS_TO_NODE,\n"+
			"AEROSPIKE_TIMEOUT,\n"+
			"AEROSPIKE_DEVICE_OVERLOAD,\n"+
			"AEROSPIKE_NETWORK_ERROR,\n"+
			"AEROSPIKE_SERVER_NOT_AVAILABLE,\n"+
			"AEROSPIKE_BATCH_FAILED,\n"+
			"AEROSPIKE_MAX_ERROR_RATE.\n"+
			"This base timeout value is also used as the interval multiplied by --retry-multiplier to increase\n"+
			"the timeout value between retry attempts.")
	flagSet.Float64Var(&f.RetryMultiplier, "retry-multiplier",
		1,
		"Increases the delay between subsequent retry attempts for the errors listed under --retry-base-timeout.\n"+
			"The actual delay is calculated as: retry-base-timeout * (retry-multiplier ^ attemptNumber)")
	flagSet.UintVar(&f.RetryMaxRetries, "retry-max-retries",
		0,
		"Set the maximum number of retry attempts for the errors listed under --retry-base-timeout.\n"+
			"The default is 0, indicating no retries will be performed")

	flagSet.StringVar(&f.Mode, "mode",
		"auto",
		"Restore mode: auto, asb, asbx. According to this parameter different restore processes wil be started.\n"+
			"auto - starts restoring from both .asb and .asbx files.\n"+
			"asb - restore only .asb backup files.\n"+
			"asbx - restore only .asbx backup files.")

	flagSet.BoolVar(&f.ValidateOnly, "validate",
		false,
		"Validate backup files without restoring.")

	return flagSet
}

func (f *Restore) GetRestore() *models.Restore {
	return &f.Restore
}
