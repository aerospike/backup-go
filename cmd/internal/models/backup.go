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

type Backup struct {
	OutputFile          string
	RemoveFiles         bool
	ModifiedBefore      string
	ModifiedAfter       string
	FileLimit           int64
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
}

// ShouldClearTarget check if we should clean target directory.
func (b *Backup) ShouldClearTarget() bool {
	return b.RemoveFiles || b.RemoveArtifacts
}
