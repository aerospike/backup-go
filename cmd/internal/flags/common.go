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

type Common struct {
	models.Common
}

func NewCommon() *Common {
	return &Common{}
}

func (f *Common) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Directory, "directory", "d",
		"",
		"The Directory that holds the backup files. Required, unless -o or -e is used.")
	flagSet.StringVarP(&f.Namespace, "namespace", "n",
		"",
		"The namespace to be backed up. Required.")
	flagSet.StringArrayVarP(&f.SetList, "set", "s",
		nil,
		"The set(s) to be backed up.\n"+
			"If multiple sets are being backed up, filter-exp cannot be used.\n"+
			"if empty all sets.")
	flagSet.IntVarP(&f.RecordsPerSecond, "records-per-second", "L",
		0,
		"Limit total returned records per second (rps).\n"+
			"Do not apply rps limit if records-per-second is zero.")
	flagSet.StringArrayVarP(&f.BinList, "bin-list", "B",
		nil,
		"Only include the given bins in the backup.\n"+
			"If empty include all bins.")
	flagSet.IntVarP(&f.Parallel, "parallel", "w",
		1,
		"Maximum number of scan calls to run in parallel.\n"+
			"If only one partition range is given, or the entire namespace is being backed up, the range\n"+
			"of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each\n"+
			"filter cannot be parallelized individually, so you may only achieve as much parallelism as there are\n"+
			"partition filters.")
	flagSet.BoolVarP(&f.NoRecords, "no-records", "R",
		false,
		"Don't backup any records.")
	flagSet.BoolVarP(&f.NoIndexes, "no-indexes", "I",
		false,
		"Don't backup any indexes.")
	flagSet.BoolVar(&f.NoUDFs, "no-udfs",
		false,
		"Don't backup any UDFs.")
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
