// Copyright 2024-2026 Aerospike, Inc.
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

// Statistics is the size of a backup, summed from the manifests the server wrote
// for it. See Lister.GetStatistics for what each count can and cannot tell.
type Statistics struct {
	// RecordCount is the number of records the base scan of the backup read, an
	// estimate of the records in the backup.
	RecordCount uint64
	// IncrRecordCount is the number of records the incremental scan of the backup
	// read. They are the records updated within the clock skew before the backup
	// started or while it ran, and most of them are also in RecordCount.
	IncrRecordCount uint64
	// ChangeRecordCount is the number of writes the change stream of the backup
	// recorded while it ran, one per write, not per record.
	ChangeRecordCount uint64
	// ByteCount is the size of the segment files of the backup, in bytes.
	ByteCount uint64
	// FileCount is the number of segment files of the backup.
	FileCount uint64
}

// Add adds the counts of other to s.
func (s *Statistics) Add(other Statistics) {
	s.RecordCount += other.RecordCount
	s.IncrRecordCount += other.IncrRecordCount
	s.ChangeRecordCount += other.ChangeRecordCount
	s.ByteCount += other.ByteCount
	s.FileCount += other.FileCount
}
