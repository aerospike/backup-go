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

	flagSet.StringVarP(&f.File, "input-file", "i",
		"",
		"Restore from a single backup file. Use - for stdin.\n"+
			"Required, unless --directory or --directory-list is used.\n")
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
			"<max-async-batches> * <batch-size>\n"+
			"Default is 32.")
	flagSet.IntVar(&f.BatchSize, "batch-size", 128,
		"The max allowed number of records to simultaneously upload\n"+
			"in an async batch write calls to make to aerospike at a time.\n"+
			"Default is 128 with batch writes enabled, or 16 without batch writes.")
	flagSet.Int64Var(&f.ExtraTTL, "extra-ttl",
		0,
		"For records with expirable void-times, add N seconds of extra-ttl to the\n"+
			"recorded void-time.")

	return flagSet
}

func (f *Restore) GetRestore() *models.Restore {
	return &f.Restore
}
