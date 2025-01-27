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
		"The Directory that holds the backup files. Required.")
	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		false,
		"Remove existing backup file (-o) or files (-d).")
	flagSet.Int64VarP(&f.FileLimit, "file-limit", "F",
		262144000, // 250 MB
		"Rotate backup files, when their size crosses the given\n"+
			"value (in bytes) Only used when backing up to a Directory. 0 - no limit.")
	flagSet.IntVar(&f.ParallelWrite, "parallel-write",
		0,
		"Number of concurrent backup files writing.\n"+
			"If not set the default value is automatically calculated and appears as the number of CPUs on your machine.")
	flagSet.StringVar(&f.DC, "dc",
		"dc",
		"DC that will be created on source instance for xdr backup.")
	flagSet.StringVar(&f.LocalAddress, "local-address",
		"127.0.0.1",
		"Local IP address on which XDR server listens on.")
	flagSet.IntVar(&f.LocalPort, "local-port",
		8080,
		"Local port on which XDR server listens on.")
	flagSet.StringVar(&f.Rewind, "rewind",
		"all",
		"Rewind is used to ship all existing records of a namespace.\n"+
			"When rewinding a namespace, XDR will scan through the index and ship\n"+
			"all the records for that namespace, partition by partition.\n"+
			"Can be `all` or number of seconds.")
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
		100,
		"Maximum number of concurrent TCP connections.")
	flagSet.Int64Var(&f.InfoPolingPeriodMilliseconds, "info-poling-period",
		1000,
		"How often (in milliseconds) a backup client will send info commands to check aerospike cluster stats.\n"+
			"To measure recovery state and lag.")
	flagSet.Int64Var(&f.StartTimeoutMilliseconds, "start-timeout",
		30000,
		"Timeout for starting TCP server for XDR.\n"+
			"If the TCP server for XDR does not receive any data within this timeout period, it will shut down.\n"+
			"This situation can occur if the --local-address and --local-port options are misconfigured.")
	flagSet.BoolVar(&f.StopXDR, "stop-xdr",
		false,
		"Stop XDR and removes XDR config from database. Is used if previous XDR backup was interrupted or failed, \n"+
			"and database server still sends XDR events. Use this functionality to stop XDR after interrupted backup.")
	flagSet.BoolVar(&f.UnblockMRT, "unblock-mrt",
		false,
		"Unblock MRT writes on the database.\n"+
			"Use this functionality to unblock MRT writes after interrupted backup.")

	return flagSet
}

func (f *BackupXDR) GetBackupXDR() *models.BackupXDR {
	return &f.BackupXDR
}
