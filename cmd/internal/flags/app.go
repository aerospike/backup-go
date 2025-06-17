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

type App struct {
	models.App
}

func NewApp() *App {
	return &App{}
}

func (f *App) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.BoolP("help", "Z", false, "Display help information.")
	flagSet.BoolVarP(&f.Version, "version", "V",
		false,
		"Display version information.")
	flagSet.BoolVarP(&f.Verbose, "verbose", "v",
		false,
		"Enable more detailed logging.")
	flagSet.StringVar(&f.LogLevel, "log-level",
		"debug",
		"Determine log level for --verbose output. Log levels are: debug, info, warn, error.")
	flagSet.BoolVar(&f.LogJSON, "log-json",
		false,
		"Set output in JSON format for parsing by external tools.")
	flagSet.StringVar(&f.Config, "config",
		"",
		"Path to YAML configuration file.")

	return flagSet
}

func (f *App) GetApp() *models.App {
	return &f.App
}
