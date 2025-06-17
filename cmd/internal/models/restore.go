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

import "fmt"

const (
	RestoreModeAuto = "auto"
	RestoreModeASB  = "asb"
	RestoreModeASBX = "asbx"
)

// Restore contains flags that will be mapped to restore config.
type Restore struct {
	Common

	InputFile          string `yaml:"input-file,omitempty"`
	DirectoryList      string `yaml:"directory-list,omitempty"`
	ParentDirectory    string `yaml:"parent-directory,omitempty"`
	DisableBatchWrites bool   `yaml:"disable-batch-writes,omitempty"`
	BatchSize          int    `yaml:"batch-size,omitempty"`
	MaxAsyncBatches    int    `yaml:"max-async-batches,omitempty"`
	// For optimal performance, should be at least MaxAsyncBatches.
	// This is applicable only to batch writes.
	WarmUp            int   `yaml:"warm-up,omitempty"`
	ExtraTTL          int64 `yaml:"extra-ttl,omitempty"`
	IgnoreRecordError bool  `yaml:"ignore-record-error,omitempty"`
	Uniq              bool  `yaml:"uniq,omitempty"`
	Replace           bool  `yaml:"replace,omitempty"`
	NoGeneration      bool  `yaml:"no-generation,omitempty"`
	TimeOut           int64 `yaml:"timeout,omitempty"`

	RetryBaseTimeout int64   `yaml:"retry-base-timeout,omitempty"`
	RetryMultiplier  float64 `yaml:"retry-multiplier,omitempty"`
	RetryMaxRetries  uint    `yaml:"retry-max-retries,omitempty"`

	Mode string `yaml:"mode,omitempty"`

	ValidateOnly bool `yaml:"validate-only,omitempty"`
}

func (r *Restore) IsDirectoryRestore() bool {
	return r.DirectoryList == "" && r.InputFile == ""
}

func (r *Restore) Validate() error {
	if r == nil {
		return nil
	}

	switch r.Mode {
	case RestoreModeAuto, RestoreModeASB, RestoreModeASBX:
		// ok.
	default:
		return fmt.Errorf("invalid restore mode: %s", r.Mode)
	}

	if r.InputFile == "" &&
		r.Directory == "" &&
		r.DirectoryList == "" {
		return fmt.Errorf("input file or directory required")
	}

	if r.Directory != "" && r.InputFile != "" {
		return fmt.Errorf("only one of directory and input-file may be configured at the same time")
	}

	if r.DirectoryList != "" && (r.Directory != "" || r.InputFile != "") {
		return fmt.Errorf("only one of directory, input-file and directory-list may be configured at the same time")
	}

	if r.ParentDirectory != "" && r.DirectoryList == "" {
		return fmt.Errorf("must specify directory-list list")
	}

	if r.WarmUp < 0 {
		return fmt.Errorf("warm-up must be non-negative")
	}

	if !r.ValidateOnly {
		// Validate common backup only if restore is not in validate only mode.
		if err := r.Common.Validate(); err != nil {
			return err
		}
	}

	return nil
}
