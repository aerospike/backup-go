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

package streamers

import (
	"github.com/aerospike/backup-go/pkg/server/internal/manifestname"
)

// decimalBase is the base of the partition id of a data directory.
const decimalBase = 10

// partitionOfManifest returns the partition a query stream manifest describes,
// read from its file name, {pid}-{regime}-{timestamp}-{uuid}.json, the way the
// server reads it when it groups manifests for a restore. ok is false for any
// other name, which the server skips on restore.
func partitionOfManifest(manifestPath string) (id int, ok bool) {
	name, ok := manifestname.ParseQuery(manifestPath)

	return name.Partition, ok
}
