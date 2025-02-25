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

type BackupXDR struct {
	models.BackupXDR
}

func NewBackupXDR() *BackupXDR {
	return &BackupXDR{}
}

func (f *BackupXDR) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		"",
		"The namespace to be backed up. Required.")

	flagSet.StringVarP(&f.Directory, "directory", "d",
		"",
		"The directory that holds the backup files. Required.")
	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		false,
		"Remove an existing backup file (-o) or entire directory (-d) and replace with the new backup.")
	flagSet.Int64VarP(&f.FileLimit, "file-limit", "F",
		262144000, // 250 MB
		"Rotate backup files when their size crosses the given\n"+
			"value (in bytes). Only used when backing up to a directory.\n")
	flagSet.IntVar(&f.ParallelWrite, "parallel-write",
		0,
		"Number of concurrent backup files writing.\n"+
			"If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.")
	flagSet.StringVar(&f.DC, "dc",
		"dc",
		"DC that will be created on source instance for xdr backup.\n"+
			"DC name can include only Latin lowercase and uppercase letters with no diacritical marks (a-z, A-Z),\n"+
			"digits 0-9, underscores (_), hyphens (-), and dollar signs ($). Max length is 31 bytes.")
	flagSet.StringVar(&f.LocalAddress, "local-address",
		"127.0.0.1",
		"Local IP address that the XDR server listens on.")
	flagSet.IntVar(&f.LocalPort, "local-port",
		8080,
		"Local port that the XDR server listens on.")
	flagSet.StringVar(&f.Rewind, "rewind",
		"all",
		"Rewind is used to ship all existing records of a namespace.\n"+
			"When rewinding a namespace, XDR will scan through the index and ship\n"+
			"all the records for that namespace, partition by partition.\n"+
			"Can be the string \"all\" or an integer number of seconds.")
	flagSet.IntVar(&f.MaxThroughput, "max-throughput",
		0,
		"Number of records per second to ship using XDR."+
			"The --max-throughput value should be in multiples of 100.\n"+
			"If 0, the default server value will be used.")
	flagSet.Int64Var(&f.ReadTimeoutMilliseconds, "read-timeout",
		1000,
		"Timeout in milliseconds for TCP read operations. Used by TCP server for XDR.")
	flagSet.Int64Var(&f.WriteTimeoutMilliseconds, "write-timeout",
		1000,
		"Timeout in milliseconds for TCP write operations. Used by TCP server for XDR.")
	flagSet.IntVar(&f.ResultQueueSize, "results-queue-size",
		256,
		"Buffer for processing messages received from XDR.")
	flagSet.IntVar(&f.AckQueueSize, "ack-queue-size",
		256,
		"Buffer for processing acknowledge messages sent to XDR.")
	flagSet.IntVar(&f.MaxConnections, "max-connections",
		256,
		"Maximum number of concurrent TCP connections.")
	flagSet.Int64Var(&f.InfoPolingPeriodMilliseconds, "info-poling-period",
		1000,
		"How often (in milliseconds) a backup client sends info commands\n"+
			"to check Aerospike cluster statistics on recovery rate and lag.")
	flagSet.Int64Var(&f.InfoRetryIntervalMilliseconds, "info-retry-timeout", 1000,
		"Set the initial timeout for a retry in milliseconds when info commands are sent.\n"+
			"This parameter is applied to stop-xdr and unblock-mrt requests.")
	flagSet.Float64Var(&f.InfoRetriesMultiplier, "info-retry-multiplier",
		1,
		"Increases the delay between subsequent retry attempts.\n"+
			"The actual delay is calculated as: info-retry-timeout * (info-retry-multiplier ^ attemptNumber)")
	flagSet.UintVar(&f.InfoMaxRetries, "info-max-retries", 3,
		"How many times to retry sending info commands before failing.\n"+
			" This parameter is applied to stop-xdr and unblock-mrt requests.")
	flagSet.Int64Var(&f.StartTimeoutMilliseconds, "start-timeout",
		30000,
		"Timeout for starting TCP server for XDR.\n"+
			"If the TCP server for XDR does not receive any data within this timeout period, it will shut down.\n"+
			"This situation can occur if the --local-address and --local-port options are misconfigured.")
	flagSet.BoolVar(&f.StopXDR, "stop-xdr",
		false,
		"Stops XDR and removes XDR configuration from the database.\n"+
			"Used if previous XDR backup was interrupted or failed, but the database server still sends XDR events.\n"+
			"Use this functionality to stop XDR after an interrupted backup.")
	flagSet.BoolVar(&f.UnblockMRT, "unblock-mrt",
		false,
		"Unblock MRT writes on the database.\n"+
			"Use this functionality to unblock MRT writes after an interrupted backup.")

	return flagSet
}

func (f *BackupXDR) GetBackupXDR() *models.BackupXDR {
	return &f.BackupXDR
}
