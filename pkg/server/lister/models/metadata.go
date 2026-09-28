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

// MetadataStatus is the state a backup was left in, as recorded by the server
// that wrote it.
type MetadataStatus string

const (
	// MetadataStatusComplete means the backup ran to the end.
	MetadataStatusComplete MetadataStatus = "complete"
	// MetadataStatusAborted means the backup was stopped before it finished.
	MetadataStatusAborted MetadataStatus = "aborted"
)

// Metadata is the metadata for a backup.
type Metadata struct {
	BackupID  string         `json:"backup_id"`
	Namespace string         `json:"namespace"`
	Status    MetadataStatus `json:"status"`
}
