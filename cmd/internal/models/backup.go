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

import (
	"fmt"
)

// Backup flags that will be mapped to (scan) backup config.
// (common for backup and restore flags are in Common).
type Backup struct {
	Common

	OutputFile          string
	RemoveFiles         bool
	ModifiedBefore      string
	ModifiedAfter       string
	FileLimit           uint64
	AfterDigest         string
	MaxRecords          int64
	NoBins              bool
	SleepBetweenRetries int
	FilterExpression    string
	ParallelNodes       bool
	RemoveArtifacts     bool
	Compact             bool
	NodeList            string
	NoTTLOnly           bool
	PreferRacks         string
	PartitionList       string
	Estimate            bool
	EstimateSamples     int64
	StateFileDst        string
	Continue            string
	ScanPageSize        int64
	OutputFilePrefix    string
	RackList            string

	InfoMaxRetries                uint
	InfoRetriesMultiplier         float64
	InfoRetryIntervalMilliseconds int64
}

// ShouldClearTarget check if we should clean target directory.
func (b *Backup) ShouldClearTarget() bool {
	return (b.RemoveFiles || b.RemoveArtifacts) && b.Continue == ""
}

func (b *Backup) ShouldSaveState() bool {
	return b.StateFileDst != "" || b.Continue != ""
}

//nolint:gocyclo // It is a long validation function.
func (b *Backup) Validate() error {
	if b == nil {
		return nil
	}

	if !b.Estimate && b.OutputFile == "" && b.Directory == "" {
		return fmt.Errorf("must specify either output-file or directory")
	}

	if b.Directory != "" && b.OutputFile != "" {
		return fmt.Errorf("only one of output-file and directory may be configured at the same time")
	}

	// Only one filter is allowed.
	if b.AfterDigest != "" && b.PartitionList != "" {
		return fmt.Errorf("only one of after-digest or partition-list can be configured")
	}

	if (b.Continue != "" || b.Estimate || b.StateFileDst != "") &&
		(b.ParallelNodes || b.NodeList != "") {
		return fmt.Errorf("saving states and calculating estimates is not possible in parallel node mode")
	}

	if b.Continue != "" && b.StateFileDst != "" {
		return fmt.Errorf("continue and state-file-dst are mutually exclusive")
	}

	if b.Estimate {
		// Estimate with filter not allowed.
		if b.PartitionList != "" ||
			b.NodeList != "" ||
			b.AfterDigest != "" ||
			b.FilterExpression != "" ||
			b.ModifiedAfter != "" ||
			b.ModifiedBefore != "" ||
			b.NoTTLOnly {
			return fmt.Errorf("estimate with any filter is not allowed")
		}
		// For estimate directory or file must not be set.
		if b.OutputFile != "" || b.Directory != "" {
			return fmt.Errorf("estimate with output-file or directory is not allowed")
		}
		// Check estimate samples size.
		if b.EstimateSamples < 0 {
			return fmt.Errorf("estimate with estimate-samples < 0 is not allowed")
		}
	}

	if b.NodeList != "" && b.RackList != "" {
		return fmt.Errorf("specify either rack-list or node-list, but not both")
	}

	// Validate nested common in the end.
	return b.Common.Validate()
}
