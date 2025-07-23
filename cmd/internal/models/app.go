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

package models

// App contains the global application flags.
type App struct {
	Help    bool
	Version bool
	Verbose bool
	// Set log level for verbose output.
	LogLevel string
	// Format logs as JSON, for parsing by external tools.
	LogJSON bool

	// ConfigFilePath is the path to the file used for tool configuration.
	ConfigFilePath string
}
