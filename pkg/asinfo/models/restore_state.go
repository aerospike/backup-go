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
	// RestoreStateFailed is terminal. The node that failed keeps it while the
	// other nodes of the job report RestoreStateAborted. A cold restore keeps
	// the namespace gated until it is released with restore-open.
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
	// RestoreKindCold is a restore into an empty namespace gated from clients.
	// The restore command prepares the namespace on every node and the server
	// starts the restore once every node is READY.
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
	// The server starts the restore once the namespace is READY on every node.
	NamespaceStateReady NamespaceState = "READY"
	// NamespaceStateRestoring is a gated namespace the restore writes into.
	NamespaceStateRestoring NamespaceState = "RESTORING"
	// NamespaceStateFailed is a gated namespace whose preparation failed or
	// whose restore was aborted. It stays gated until restore-open.
	NamespaceStateFailed NamespaceState = "FAILED"
	// NamespaceStateOpening is a namespace being released after a complete restore.
	NamespaceStateOpening NamespaceState = "OPENING"
	// NamespaceStateUnknown should not occur.
	NamespaceStateUnknown NamespaceState = "UNKNOWN"
	// NamespaceStateStub is reported by a stub row: a job that never got its
	// parameters, or the placeholder of a node that restarted.
	NamespaceStateStub NamespaceState = infoNoValue

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
// node that reports more than a stub row is READY.
//
// The stub rows are skipped. The states are resolved by priority: FAILED,
// then a state this package does not know (it cannot be proven to be READY,
// but must not hide a failure), RESTORING, PREPARING, OPENING, NONE and READY
// last. An empty state is returned when no node reports a state.
func ResolveNamespaceState(states []NamespaceState) NamespaceState {
	return resolveByPriority(states, namespaceStatePriority, namespaceStateUnknown,
		func(state NamespaceState) bool {
			return state == "" || state == NamespaceStateStub
		})
}

// ClearState is the storage clear state of a cold restore on a node, the clear
// field of the restore-status info response.
type ClearState string

const (
	// ClearStateNone is a node that needs no clear, or whose last clear finished.
	ClearStateNone ClearState = "none"
	// ClearStateRequired is a node that must be cleared with
	// restore-clear-storage before its namespace can reach READY.
	ClearStateRequired ClearState = "required"
	// ClearStateClearing is a node whose clear was accepted and is running.
	ClearStateClearing ClearState = "clearing"

	// clearStateUnknown holds the place of the states this package does not
	// know in clearStatePriority. The server never reports it.
	clearStateUnknown ClearState = "<unknown>"
)

// clearStatePriority orders the clear states for [ResolveClearState].
// Lower index = higher priority.
var clearStatePriority = []ClearState{
	ClearStateRequired,
	clearStateUnknown,
	ClearStateClearing,
	ClearStateNone,
}

// ResolveClearState picks a single clear state out of the states reported by
// the nodes, so that the cluster is only reported as needing no clear when no
// node needs or runs one.
//
// The nodes that report no state, such as the rows of a fuzzy restore, are
// skipped. The states are resolved by priority: required, then a state this
// package does not know (it cannot be proven to need no clear), clearing and
// none last. An empty state is returned when no node reports a state.
func ResolveClearState(states []ClearState) ClearState {
	return resolveByPriority(states, clearStatePriority, clearStateUnknown,
		func(state ClearState) bool { return state == "" })
}

// ResolveRestoreState returns the combined restore job state across nodes.
//
// A job with FAILED, ABORTING or ABORTED on any node is unusable: it is
// ABORTING while any node is not terminal yet, a state this package does not
// know included, and once every node is terminal it is FAILED if any node
// failed, and ABORTED otherwise. Otherwise a state this package does not know,
// UNKNOWN included, makes the whole job UNKNOWN, the job is ACTIVE while any
// node is ACTIVE, and COMPLETE only when every node is COMPLETE.
func ResolveRestoreState(states []RestoreState) RestoreState {
	switch {
	case len(states) == 0:
		return RestoreStateUnknown
	case slices.ContainsFunc(states, isRestoreStateStopping):
		switch {
		case !all(states, isRestoreStateTerminal):
			return RestoreStateAborting
		case slices.Contains(states, RestoreStateFailed):
			return RestoreStateFailed
		default:
			return RestoreStateAborted
		}
	case !all(states, isRestoreStateRunning):
		return RestoreStateUnknown
	case slices.Contains(states, RestoreStateActive):
		return RestoreStateActive
	default:
		return RestoreStateComplete
	}
}

// isRestoreStateStopping reports whether state is one of the states of a job
// that stops without completing: FAILED, ABORTING or ABORTED.
func isRestoreStateStopping(state RestoreState) bool {
	return state == RestoreStateFailed || state == RestoreStateAborting || state == RestoreStateAborted
}

// isRestoreStateTerminal reports whether state is terminal on its node:
// COMPLETE, FAILED or ABORTED.
func isRestoreStateTerminal(state RestoreState) bool {
	return state == RestoreStateComplete || state == RestoreStateFailed || state == RestoreStateAborted
}

// isRestoreStateRunning reports whether state is one of the states of a job
// that has not stopped: ACTIVE or COMPLETE.
func isRestoreStateRunning(state RestoreState) bool {
	return state == RestoreStateActive || state == RestoreStateComplete
}

// all reports whether f reports true for every element of s.
func all[T any](s []T, f func(T) bool) bool {
	return !slices.ContainsFunc(s, func(v T) bool { return !f(v) })
}

// resolveByPriority returns the state of the highest priority among states.
// priority lists the known states from the highest priority down and holds
// unknown in the place of the states it does not list. The states for which
// skip reports true are ignored; the zero value is returned when none is left.
func resolveByPriority[T comparable](states, priority []T, unknown T, skip func(T) bool) T {
	var (
		resolved     T
		resolvedRank = len(priority)
		unknownRank  = slices.Index(priority, unknown)
	)

	for _, state := range states {
		if skip(state) {
			continue
		}

		rank := slices.Index(priority, state)
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
