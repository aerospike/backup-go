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
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type Backup struct {
	models.Backup
}

func NewBackup() *Backup {
	return &Backup{}
}

func (f *Backup) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		"",
		"The namespace to be backed up. Required.")
	flagSet.StringArrayVarP(&f.SetList, "set", "s",
		nil,
		"The set(s) to be backed up.\n"+
			"If multiple sets are being backed up, filter-exp cannot be used.\n"+
			"Default: all sets.")
	flagSet.Int64VarP(&f.FileLimit, "file-limit", "F",
		0,
		"Rotate backup files, when their size crosses the given\n"+
			"value (in bytes) Only used when backing up to a Directory. "+
			"Default: 0.")
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		0,
		"Limit total returned records per second (rps).\n"+
			"Do not apply rps limit if records-per-second is zero.\n"+
			"Default: 0.")
	flagSet.StringArrayVarP(&f.BinList, "bin-list", "B",
		nil,
		"Only include the given bins in the backup.\n"+
			"Default: include all bins.")
	flagSet.IntVarP(&f.Parallel, "parallel", "w",
		1,
		"Maximum number of scan calls to run in parallel.\n"+
			"If only one partition range is given, or the entire namespace is being backed up, the range\n"+
			"of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each\n"+
			"filter cannot be parallelized individually, so you may only achieve as much parallelism as there are\n"+
			"partition filters.\n"+
			"Default: 1")
	flagSet.StringVarP(&f.AfterDigest, "after-digest", "D",
		"",
		"Backup records after record digest in record's partition plus all succeeding\n"+
			"partitions. Used to resume backup with last record received from previous\n"+
			"incomplete backup.\n"+
			"This argument is mutually exclusive to partition-list.\n"+
			"Format: base64 encoded string\n"+
			"Example: EjRWeJq83vEjRRI0VniavN7xI0U=\n")
	flagSet.BoolVarP(&f.NoRecords, "no-records", "R",
		false,
		"Don't backup any records.")
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "I",
		false,
		"Don't backup any indexes.")
	flagSet.BoolVarP(&f.NoUDFs, "no-udfs", "u",
		false,
		"Don't backup any UDFs.")
	flagSet.StringVarP(&f.ModifiedBefore, "modified-before", "a",
		"",
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Perform an incremental backup; only include records \n"+
			"that changed after the given date and time. The system's \n"+
			"local timezone applies. If only HH:MM:SS is specified, then\n"+
			"today's date is assumed as the date. If only YYYY-MM-DD is \n"+
			"specified, then 00:00:00 (midnight) is assumed as the time.\n")
	flagSet.StringVarP(&f.ModifiedAfter, "modified-after", "b",
		"",
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Only include records that last changed before the given\n"+
			"date and time. May combined with --modified-after to specify a range.")
	flagSet.IntVar(&f.MaxRetries, "max-retries",
		5,
		"Maximum number of retries before aborting the current transaction.\n"+
			"Default: 5")
	flagSet.Int64VarP(&f.MaxRecords, "max-records", "M",
		0,
		"The number of records approximately to back up.\n"+
			"Default: all records")
	flagSet.BoolVarP(&f.NoBins, "no-bins", "x",
		false,
		"Do not include bin data in the backup.")
	flagSet.IntVar(&f.SleepBetweenRetries, "sleep-between-retries",
		5,
		"The amount of milliseconds to sleep between retries.\n"+
			"Default: 0")
	flagSet.StringVarP(&f.FilterExpression, "filter-exp", "f",
		"",
		"Base64 encoded expression. Use the encoded filter expression in each scan call,\n"+
			"which can be used to do a partial backup. The expression to be used can be base64 \n"+
			"encoded through any client. This argument is mutually exclusive with multi-set backup.\n")
	flagSet.IntVar(&f.TotalTimeout, "total-timeout",
		0,
		"Total socket timeout in milliseconds.\n"+
			"Default: 0 - no timeout.")
	flagSet.IntVar(&f.SocketTimeout, "socket-timeout",
		10000,
		"Socket timeout in milliseconds. If this value is 0, its set to total-timeout. If both are 0,\n"+
			"there is no socket idle time limit\n"+
			"Default: 10 seconds.")

	return flagSet
}

func (f *Backup) GetBackup() *models.Backup {
	return &f.Backup
}
