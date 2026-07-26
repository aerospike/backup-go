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

package asinfo

// Namespace restore states returned by the restore-status info command.
const (
	RestoreStateNone      = "NONE"
	RestoreStatePreparing = "PREPARING"
	RestoreStateReady     = "READY"
	RestoreStateRestoring = "RESTORING"
	RestoreStateFailed    = "FAILED"
	NsRestoreStateUnknown = "UNKNOWN"
)

// restoreStatePriority defines priority for "active" states when
// nodes disagree. Lower index = higher priority.
var restoreStatePriority = []string{
	RestoreStateFailed,
	RestoreStateRestoring,
	RestoreStatePreparing,
}

// resolveRestoreState picks a single state out of the states seen
// across all nodes.
//
// Priority:
//  1. If any node reports an active state (PREPARING, RESTORING, FAILED),
//     return the highest-priority one among those seen.
//  2. Otherwise all nodes report READY or NONE; return NONE if any node
//     reports NONE, otherwise READY.
func resolveRestoreState(seen map[string]struct{}) string {
	for _, state := range restoreStatePriority {
		if _, ok := seen[state]; ok {
			return state
		}
	}

	if _, ok := seen[RestoreStateNone]; ok {
		return RestoreStateNone
	}

	return RestoreStateReady
}
