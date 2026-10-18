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

import "slices"

// RestoreState is the state of a restore job on a node, the state field of the
// restore-status info response.
type RestoreState string

const (
	RestoreStateActive   RestoreState = "ACTIVE"
	RestoreStateComplete RestoreState = "COMPLETE"
	// RestoreStateFailed is normally followed by RestoreStateAborted once the
	// principal aborts the job.
	RestoreStateFailed   RestoreState = "FAILED"
	RestoreStateAborting RestoreState = "ABORTING"
	// RestoreStateAborted is terminal. A cold restore keeps the namespace gated
	// until it is released with restore-open.
	RestoreStateAborted RestoreState = "ABORTED"
	RestoreStateUnknown RestoreState = "UNKNOWN"
)

// RestoreKind is the mode of a restore job, the kind field of the
// restore-status info response.
type RestoreKind string

const (
	// RestoreKindCold is a restore into an empty namespace gated from clients,
	// prepared with restore-prepare.
	RestoreKindCold RestoreKind = "cold"
	// RestoreKindFuzzy is a restore into a live namespace through the normal
	// write path.
	RestoreKindFuzzy RestoreKind = "fuzzy"
)

// NamespaceState is the restore gate of a namespace on a node, the ns-state
// field of the restore-status info response. Only a cold restore changes it.
type NamespaceState string

const (
	// NamespaceStateNone is a namespace without a cold restore, open to clients.
	NamespaceStateNone NamespaceState = "NONE"
	// NamespaceStatePreparing is a gated namespace that is not empty yet.
	NamespaceStatePreparing NamespaceState = "PREPARING"
	// NamespaceStateReady is a gated, empty namespace, ready for the restore.
	NamespaceStateReady NamespaceState = "READY"
	// NamespaceStateRestoring is a gated namespace the restore writes into.
	NamespaceStateRestoring NamespaceState = "RESTORING"
	// NamespaceStateFailed is a gated namespace whose preparation failed or
	// whose restore was aborted. It stays gated until restore-open.
	NamespaceStateFailed NamespaceState = "FAILED"
	// NamespaceStateOpening is a namespace being released after a complete restore.
	NamespaceStateOpening NamespaceState = "OPENING"
	// NamespaceStateNotHosted is reported by a node that does not host the
	// namespace, and for a declined start.
	NamespaceStateNotHosted NamespaceState = infoNoValue

	// namespaceStateUnknown holds the place of the states this package does
	// not know in namespaceStatePriority. The server never reports it.
	namespaceStateUnknown NamespaceState = "<unknown>"
)

// namespaceStatePriority orders the namespace states for [ResolveNamespaceState].
// Lower index = higher priority.
var namespaceStatePriority = []NamespaceState{
	NamespaceStateFailed,
	namespaceStateUnknown,
	NamespaceStateRestoring,
	NamespaceStatePreparing,
	NamespaceStateOpening,
	NamespaceStateNone,
	NamespaceStateReady,
}

// ResolveNamespaceState picks a single namespace state out of the states
// reported by the nodes, so that the cluster is only reported READY when every
// node hosting the namespace is READY.
//
// The nodes that do not host the namespace are skipped. The states are
// resolved by priority: FAILED, then a state this package does not know (it
// cannot be proven to be READY, but must not hide a failure), RESTORING,
// PREPARING, OPENING, NONE and READY last. An empty state is returned when no
// node reports a state.
func ResolveNamespaceState(states []NamespaceState) NamespaceState {
	var (
		resolved     NamespaceState
		resolvedRank = len(namespaceStatePriority)
		unknownRank  = slices.Index(namespaceStatePriority, namespaceStateUnknown)
	)

	for _, state := range states {
		if state == "" || state == NamespaceStateNotHosted {
			continue
		}

		rank := slices.Index(namespaceStatePriority, state)
		if rank < 0 {
			rank = unknownRank
		}

		if rank < resolvedRank {
			resolvedRank = rank
			resolved = state
		}
	}

	return resolved
}

// ResolveRestoreState returns the combined restore job state across nodes.
// FAILED on any node makes the whole job failed, then ABORTING, ABORTED and
// ACTIVE win in that order. The job is COMPLETE only when it is COMPLETE on
// every node; any other combination is UNKNOWN.
func ResolveRestoreState(states []RestoreState) RestoreState {
	for _, state := range []RestoreState{
		RestoreStateFailed,
		RestoreStateAborting,
		RestoreStateAborted,
		RestoreStateActive,
	} {
		if slices.Contains(states, state) {
			return state
		}
	}

	if len(states) > 0 && !slices.ContainsFunc(states, func(s RestoreState) bool {
		return s != RestoreStateComplete
	}) {
		return RestoreStateComplete
	}

	return RestoreStateUnknown
}
