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
	// Additional.
	Verbose bool
	// Map to backup.BackupConfig.ModBefore and ModAfter
	ModifiedBefore string
	ModifiedAfter  string

	Namespace string
	SetList   []string
	BinList   []string

	//	Partitions PartitionRange

	Parallel         int
	NoRecords        bool
	NoIndexes        bool
	NoUDFs           bool
	RecordsPerSecond int
	// 	Bandwidth int
	FileLimit   int64
	AfterDigest string
	// Scan policy params.
	MaxRetries          int
	MaxRecords          int64
	NoBins              bool
	SleepBetweenRetries int
	FilterExpression    string
	TotalTimeout        int
	SocketTimeout       int
}
