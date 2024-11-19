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

const (
	// OperationBackup is used to generate proper documentation for backup.
	OperationBackup = iota
	// OperationRestore is used to generate proper documentation for restore.
	OperationRestore

	descNamespaceBackup  = "The namespace to be backed up. Required."
	descNamespaceRestore = "Used to restore to a different namespace. Example: source-ns,destination-ns"

	descSetListBackup = "The set(s) to be backed up.\n" +
		"If multiple sets are being backed up, filter-exp cannot be used.\n" +
		"if empty all sets."
	descSetListRestore = "Only restore the given sets from the backup.\n" +
		"Default: restore all sets."

	descBinListBackup = "Only include the given bins in the backup.\n" +
		"If empty include all bins."
	descBinListRestore = "Only restore the given bins in the backup.\n" +
		"Default: restore all bins.\n"

	descNoRecordsBackup  = "Don't backup any records."
	descNoRecordsRestore = "Don't restore any records."

	descNoIndexesBackup  = "Don't backup any indexes."
	descNoIndexesRestore = "Don't restore any secondary indexes."

	descNoUDFsBackup  = "Don't backup any UDFs."
	descNoUDFsRestore = "Don't restore any UDFs."

	descParallelBackup = "Maximum number of scan calls to run in parallel.\n" +
		"If only one partition range is given, or the entire namespace is being backed up, the range\n" +
		"of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each\n" +
		"filter cannot be parallelized individually, so you may only achieve as much parallelism as there are\n" +
		"partition filters."
	descParallelRestore = "The number of restore threads."
)

type Common struct {
	// operation: backup or restore, to form correct documentation.
	operation int
	models.Common
}

func NewCommon(operation int) *Common {
	return &Common{operation: operation}
}

func (f *Common) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	var descNamespace, descSetList, descBinList, descNoRecords, descNoIndexes, descNoUDFs, descParallel string

	switch f.operation {
	case 0:
		descNamespace = descNamespaceBackup
		descSetList = descSetListBackup
		descBinList = descBinListBackup
		descNoRecords = descNoRecordsBackup
		descNoIndexes = descNoIndexesBackup
		descNoUDFs = descNoUDFsBackup
		descParallel = descParallelBackup
	case 1:
		descNamespace = descNamespaceRestore
		descSetList = descSetListRestore
		descBinList = descBinListRestore
		descNoRecords = descNoRecordsRestore
		descNoIndexes = descNoIndexesRestore
		descNoUDFs = descNoUDFsRestore
		descParallel = descParallelRestore
	}

	flagSet.StringVarP(&f.Directory, "directory", "d",
		"",
		"The Directory that holds the backup files. Required, unless -o or -e is used.")
	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		"",
		descNamespace)
	flagSet.StringVarP(&f.SetList, "set", "s",
		"",
		descSetList)
	flagSet.StringVarP(&f.BinList, "bin-list", "B",
		"",
		descBinList)
	flagSet.BoolVarP(&f.NoRecords, "no-records", "R",
		false,
		descNoRecords)
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "I",
		false,
		descNoIndexes)
	flagSet.BoolVar(&f.NoUDFs, "no-udfs",
		false,
		descNoUDFs)
	flagSet.IntVarP(&f.Parallel, "parallel", "w",
		1,
		descParallel)
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		0,
		"Limit total returned records per second (rps).\n"+
			"Do not apply rps limit if records-per-second is zero.")
	flagSet.IntVar(&f.MaxRetries, "max-retries",
		5,
		"Maximum number of retries before aborting the current transaction.")
	flagSet.Int64Var(&f.TotalTimeout, "total-timeout",
		0,
		"Total socket timeout in milliseconds. 0 - no timeout.")
	flagSet.Int64Var(&f.SocketTimeout, "socket-timeout",
		10000,
		"Socket timeout in milliseconds. If this value is 0, its set to total-timeout. If both are 0,\n"+
			"there is no socket idle time limit")
	flagSet.IntVarP(&f.Nice, "nice", "N",
		0,
		"The limits for read/write storage bandwidth in MiB/s")

	return flagSet
}

func (f *Common) GetCommon() *models.Common {
	return &f.Common
}
