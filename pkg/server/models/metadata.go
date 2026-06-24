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

import "time"

// Metadata is the metadata for a backup.
type Metadata struct {
	BackupID      string `json:"backup_id"`
	Namespace     string `json:"namespace"`
	FormatVersion int    `json:"format_version"`
	Nodes         []Node `json:"nodes"`
}

// Node is the metadata for a node.
type Node struct {
	NodeID         string    `json:"node_id"`
	Created        time.Time `json:"created"`
	Finished       time.Time `json:"finished"`
	RecordCount    int64     `json:"record_count"`
	ByteCount      int64     `json:"byte_count"`
	PartitionCount int       `json:"partition_count"`
	SegmentCount   int       `json:"segment_count"`
}
