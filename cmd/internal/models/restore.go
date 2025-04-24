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

const (
	RestoreModeAuto = "auto"
	RestoreModeASB  = "asb"
	RestoreModeASBX = "asbx"
)

// Restore contains flags that will be mapped to restore config.
type Restore struct {
	InputFile          string
	DirectoryList      string
	ParentDirectory    string
	DisableBatchWrites bool
	BatchSize          int
	MaxAsyncBatches    int
	// For optimal performance, should be at least MaxAsyncBatches.
	// This is applicable only to batch writes.
	WarmUp            int
	ExtraTTL          int64
	IgnoreRecordError bool
	Uniq              bool
	Replace           bool
	NoGeneration      bool
	TimeOut           int64

	RetryBaseTimeout int64
	RetryMultiplier  float64
	RetryMaxRetries  uint

	Mode string
}

func (restore *Restore) IsDirectoryRestore() bool {
	return restore.DirectoryList == "" && restore.InputFile == ""
}
