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

type Backup struct {
	models.Backup
}

func NewBackup() *Backup {
	return &Backup{}
}

func (f *Backup) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.File, "output-file", "o",
		"",
		"Backup to a single backup file. Use - for stdout. Required, unless -d or -e is used.")
	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		false,
		"Remove existing backup file (-o) or files (-d).")
	flagSet.Int64VarP(&f.FileLimit, "file-limit", "F",
		0,
		"Rotate backup files, when their size crosses the given\n"+
			"value (in bytes) Only used when backing up to a Directory. "+
			"Default: 0.")
	flagSet.StringVarP(&f.AfterDigest, "after-digest", "D",
		"",
		"Backup records after record digest in record's partition plus all succeeding\n"+
			"partitions. Used to resume backup with last record received from previous\n"+
			"incomplete backup.\n"+
			"This argument is mutually exclusive to partition-list.\n"+
			"Format: base64 encoded string\n"+
			"Example: EjRWeJq83vEjRRI0VniavN7xI0U=\n")
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

	return flagSet
}

func (f *Backup) GetBackup() *models.Backup {
	return &f.Backup
}
