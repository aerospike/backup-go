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

package app

import (
	"fmt"
	"log/slog"
	"os"
)

func NewLogger(level string, isVerbose, isJSON bool) (*slog.Logger, error) {
	loggerOpt := &slog.HandlerOptions{}

	if isVerbose {
		var logLvl slog.Level

		err := logLvl.UnmarshalText([]byte(level))
		if err != nil {
			return nil, fmt.Errorf("invalid log level: %w", err)
		}

		loggerOpt.Level = logLvl
	}

	switch isJSON {
	case true:
		return slog.New(slog.NewJSONHandler(os.Stdout, loggerOpt)), nil
	default:
		return slog.New(slog.NewTextHandler(os.Stdout, loggerOpt)), nil
	}
}
