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

type Compression struct {
	models.Compression
}

func NewCompression() *Compression {
	return &Compression{}
}

func (f *Compression) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}
	flagSet.StringVarP(&f.Mode, "compress", "z",
		"NONE",
		"Enables compressing of backup files using the specified compression algorithm.\n"+
			"Supported compression algorithms are: zstd, none\n"+
			"Set the zstd compression level via the --compression-level option. Default level is 3.")
	flagSet.IntVar(&f.Level, "compression-level",
		3,
		"zstd compression level.")

	return flagSet
}

func (f *Compression) GetCompression() *models.Compression {
	return &f.Compression
}
