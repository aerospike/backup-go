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

	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		false,
		"Remove an existing backup file (-o) or entire directory (-d) and replace with the new backup.")
	flagSet.BoolVar(&f.RemoveArtifacts, "remove-artifacts",
		false,
		"Remove existing backup file (-o) or files (-d) without performing a backup.")
	flagSet.StringVarP(&f.OutputFile, "output-file", "o",
		"",
		"Backup to a single backup file. Use - for stdout. Required, unless -d or -e is used.")
	flagSet.StringVarP(&f.OutputFilePrefix, "output-file-prefix", "q",
		"",
		"When using directory parameter, prepend a prefix to the names of the generated files.")

	flagSet.Uint64VarP(&f.FileLimit, "file-limit", "F",
		262144000, // 250 MB
		"Rotate backup files when their size crosses the given\n"+
			"value (in bytes). Only used when backing up to a directory.\n")
	flagSet.BoolVarP(&f.NoBins, "no-bins", "x",
		false,
		"Do not include bin data in the backup. Use this flag for data sampling or troubleshooting.\n"+
			"On restore all records, that don't contain bin data will be skipped.")
	flagSet.BoolVar(&f.NoTTLOnly, "no-ttl-only",
		false,
		"Only include records that have no ttl set (persistent records).")
	flagSet.StringVarP(&f.AfterDigest, "after-digest", "D",
		"",
		"Backup records after record digest in record's partition plus all succeeding\n"+
			"partitions. Used to resume backup with last record received from previous\n"+
			"incomplete backup.\n"+
			"This argument is mutually exclusive to partition-list.\n"+
			"Format: Base64 encoded string\n"+
			"Example: EjRWeJq83vEjRRI0VniavN7xI0U=\n")
	flagSet.StringVarP(&f.ModifiedAfter, "modified-after", "a",
		"",
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Perform an incremental backup; only include records \n"+
			"that changed after the given date and time. The system's \n"+
			"local timezone applies. If only HH:MM:SS is specified, then\n"+
			"today's date is assumed as the date. If only YYYY-MM-DD is \n"+
			"specified, then 00:00:00 (midnight) is assumed as the time.\n")
	flagSet.StringVarP(&f.ModifiedBefore, "modified-before", "b",
		"",
		"<YYYY-MM-DD_HH:MM:SS>\n"+
			"Only include records that last changed before the given\n"+
			"date and time. May combined with --modified-after to specify a range.")
	flagSet.StringVarP(&f.FilterExpression, "filter-exp", "f",
		"",
		"Base64 encoded expression. Use the encoded filter expression in each scan call,\n"+
			"which can be used to do a partial backup. The expression to be used can be Base64 \n"+
			"encoded through any client. This argument is mutually exclusive with multi-set backup.\n")
	flagSet.BoolVar(&f.ParallelNodes, "parallel-nodes",
		false,
		"Specifies how to perform the query of the database run for each backup.\n"+
			"By default, asbackup runs parallel workers for partitions.\n"+
			"If this flag is set to true, asbackup launches parallel workers for nodes.\n"+
			"The number of parallel workers is set by the --parallel flag.\n"+
			"This option is mutually exclusive with --continue and --estimate.")
	flagSet.StringVarP(&f.NodeList, "node-list", "l",
		"",
		"<IP addr 1>:<port 1>[,<IP addr 2>:<port 2>[,...]]\n"+
			"<IP addr 1>:<TLS_NAME 1>:<port 1>[,<IP addr 2>:<TLS_NAME 2>:<port 2>[,...]]\n"+
			"Back up the given cluster nodes only.\n"+
			"The job is parallelized by number of nodes unless --parallel is set less than nodes number.\n"+
			"This argument is mutually exclusive with --partition-list and --after-digest arguments.\n"+
			"Default: backup all nodes in the cluster")
	flagSet.StringVarP(&f.PartitionList, "partition-list", "X",
		"",
		"List of partitions <filter[,<filter>[...]]> to back up. Partition filters can be ranges,\n"+
			"individual partitions, or records after a specific digest within a single partition.\n"+
			"This argument is mutually exclusive to after-digest.\n"+
			"Filter: <begin partition>[-<partition count>]|<digest>\n"+
			"begin partition: 0-4095\n"+
			"partition count: 1-4096 Default: 1\n"+
			"digest: Base64 encoded string\n"+
			"Examples: 0-1000, 1000-1000, 2222, EjRWeJq83vEjRRI0VniavN7xI0U=\n"+
			"Default: 0-4096 (all partitions)\n")
	flagSet.StringVar(&f.PreferRacks, "prefer-racks",
		"",
		"<rack id 1>[,<rack id 2>[,...]]\n"+
			"A list of Aerospike Database rack IDs to prefer when reading records for a backup.")
	flagSet.Int64VarP(&f.MaxRecords, "max-records", "M",
		0,
		"The number of records approximately to back up. 0 - all records")
	flagSet.IntVar(&f.SleepBetweenRetries, "sleep-between-retries",
		5,
		"The amount of milliseconds to sleep between retries after an error.\n"+
			"This field is ignored when --max-retries is zero.")
	flagSet.BoolVarP(&f.Compact, "compact", "C",
		false,
		"If true, do not apply base-64 encoding to BLOBs and instead write raw binary data,\n"+
			"resulting in smaller backup files.\n"+
			"Deprecated.")
	flagSet.BoolVarP(&f.Estimate, "estimate", "e",
		false,
		"Estimate the backed-up record size from a random sample of \n"+
			"10,000 (default) records at 99.9999% confidence to estimate the full backup size.\n"+
			"It ignores any filter:  --filter-exp, --node-list, --modified-after, --modified-before, --no-ttl-only,\n"+
			"--after-digest, --partition-list.")
	flagSet.Int64Var(&f.EstimateSamples, "estimate-samples",
		10000,
		"The number of samples to take when running a backup estimate.")
	flagSet.StringVarP(&f.Continue, "continue", "c",
		"",
		"Resumes an interrupted/failed backup from where it was left off, given the .state file\n"+
			"that was generated from the interrupted/failed run.")
	flagSet.StringVar(&f.StateFileDst, "state-file-dst",
		"",
		"Name of a state file that will be saved in backup --directory.\n"+
			"Works only with --file-limit parameter. As --file-limit is reached and the file is closed,\n"+
			"the current state will be saved. Works only for default and/or partition backup.\n"+
			"Not work with --parallel-nodes or --node--list.")
	flagSet.Int64Var(&f.ScanPageSize, "scan-page-size",
		10000,
		"Number of records will be read on one iteration for continuation backup.\n"+
			"Affects size if overlap on resuming backup after an error.\n"+
			"Used only with --state-file-dst or --continue.")
	flagSet.Int64Var(&f.InfoRetryIntervalMilliseconds, "info-retry-timeout", 1000,
		"Set the initial timeout for a retry in milliseconds when info commands are sent."+
			"This parameter is applied to stop xdr and unblock MRT writes requests.")
	flagSet.Float64Var(&f.InfoRetriesMultiplier, "info-retry-multiplier",
		1,
		"Increases the delay between subsequent retry attempts.\n"+
			"The actual delay is calculated as: info-retry-timeout * (info-retry-multiplier ^ attemptNumber)")
	flagSet.UintVar(&f.InfoMaxRetries, "info-max-retries", 3,
		"How many times to retry to send info commands before failing. "+
			"This parameter is applied to stop xdr and unblock MRT writes requests.")

	return flagSet
}

func (f *Backup) GetBackup() *models.Backup {
	return &f.Backup
}
