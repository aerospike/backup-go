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
	"sync/atomic"
)

// BackupStats stores the status of a backup job.
// Stats are updated in realtime by backup jobs.
type BackupStats struct {
	*commonStats
	fileCount atomic.Uint64
	// total number of records in database
	TotalRecords uint64
}

// NewBackupStats returns new backup stats.
func NewBackupStats() *BackupStats {
	return &BackupStats{}
}

// IncFiles increments by one the number of files per backup.
func (b *BackupStats) IncFiles() {
	b.fileCount.Add(1)
}

// GetFileCount returns the number of files per backup.
func (b *BackupStats) GetFileCount() uint64 {
	return b.fileCount.Load()
}

// IsEmpty determines whether the BackupStats is empty.
func (b *BackupStats) IsEmpty() bool {
	return b.GetUDFs() == 0 &&
		b.GetSIndexes() == 0 &&
		b.GetReadRecords() == 0
}

// SumBackupStats combines two BackupStats into a new one
func SumBackupStats(a, b *BackupStats) *BackupStats {
	result := &BackupStats{
		commonStats: sumCommonStats(a.commonStats, b.commonStats),
	}

	// Backup specific stats
	result.fileCount.Store(a.GetFileCount() + b.GetFileCount())
	result.TotalRecords = a.TotalRecords + b.TotalRecords

	return result
}
