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

import "time"

const (
	// infoTimeLayout is the layout the server uses for the job timestamps of
	// the backup-status and restore-status responses, e.g.
	// 2026-10-18 01.00.00.123 UTC.
	infoTimeLayout = "2006-01-02 15.04.05.000 UTC"

	// infoNoValue is emitted by the server in place of a value that is not
	// set, e.g. the finish-time of a running job.
	infoNoValue = "-"
	// infoNoName is emitted by the server in place of the job id or the
	// namespace of a stub row, a job that failed before its parameters were
	// known.
	infoNoName = "(none)"
)

// knownName reports whether name, the job id or the namespace of a status row,
// is set: a stub row reports "(none)" or "-" in place of it.
func knownName(name string) bool {
	return name != "" && name != infoNoName && name != infoNoValue
}

// Keys shared by the backup-status and restore-status info responses. Optional
// fields appear and disappear depending on the job state, so a response must be
// read by key and never by position.
const (
	fieldJobID       = "job-id"
	fieldNamespace   = "ns"
	fieldState       = "state"
	fieldStartTime   = "start-time"
	fieldFinishTime  = "finish-time"
	fieldErrorReason = "error-reason"
	fieldAbortOrigin = "abort-origin"
	fieldRecsPerSec  = "recs-per-sec"
	fieldLastLapMs   = "last-lap-ms"
)

// AbortOrigin tells what aborted a backup or restore job.
type AbortOrigin string

const (
	// AbortOriginOperator is an abort requested with backup-abort or restore-abort.
	AbortOriginOperator AbortOrigin = "operator"
	// AbortOriginServer is an abort caused by a failure on a node.
	AbortOriginServer AbortOrigin = "server"
	// AbortOriginCluster is an abort caused by a cluster event.
	AbortOriginCluster AbortOrigin = "cluster"
)

// Consistency is the consistency of a backup: the achieved-consistency field
// of the backup-status info response, and the source-consistency field of the
// restore-status info response. It is empty while the server reports none.
type Consistency string

const (
	// ConsistencyStrict is a backup of every record as of a common point.
	ConsistencyStrict Consistency = "strict"
	// ConsistencyFuzzy is a backup of the base scan only, every record as of its
	// own scan, with no common point.
	ConsistencyFuzzy Consistency = "fuzzy"

	// consistencyUnknown holds the place of the values this package does not
	// know in consistencyPriority. The server never reports it.
	consistencyUnknown Consistency = "<unknown>"
)

// consistencyPriority orders the consistencies for [ResolveConsistency].
// Lower index = higher priority.
var consistencyPriority = []Consistency{
	ConsistencyFuzzy,
	consistencyUnknown,
	ConsistencyStrict,
}

// ResolveConsistency picks a single consistency out of the values reported by
// the nodes, so that the cluster is only reported strict when no node reports
// less. The nodes that report none are skipped. The values are resolved by
// priority: fuzzy, then a value this package does not know (it cannot be
// proven to be strict), and strict last. An empty value is returned when no
// node reports one.
func ResolveConsistency(values []Consistency) Consistency {
	return resolveByPriority(values, consistencyPriority, consistencyUnknown,
		func(c Consistency) bool { return c == "" })
}

// infoValue returns the value of key, or an empty string if the server reports
// it as not set.
func infoValue(r InfoMap, key string) string {
	v := r[key]
	if v == infoNoValue {
		return ""
	}

	return v
}

// parseOptionalUint64 returns the value of key, or nil if it is missing,
// reported as not set, or unparsable.
func parseOptionalUint64(r InfoMap, key string) *uint64 {
	v, ok, err := r.ParseUint64(key)
	if !ok || err != nil {
		return nil
	}

	return &v
}

// maxOptional returns the larger of two optional values, ignoring a nil one.
// The result never aliases b, so a merged status does not share memory with
// the status of a node.
func maxOptional(a, b *uint64) *uint64 {
	if b == nil || (a != nil && *a >= *b) {
		return a
	}

	v := *b

	return &v
}

// infoField binds a key of an info response to the struct field it fills.
type infoField[T any] struct {
	key string
	dst *T
}

// infoFieldOf returns the infoField that fills dst from key.
func infoFieldOf[T any](key string, dst *T) infoField[T] {
	return infoField[T]{key: key, dst: dst}
}

// parseInfoFields fills every field from the info map with parse. A field the
// server did not report, or reported in an unparsable form, is left unchanged:
// the optional fields of a response make a missing key an expected case, and a
// single bad value must not discard the rest of the status.
func parseInfoFields[T any](r InfoMap, parse func(InfoMap, string) (T, bool, error), fields ...infoField[T]) {
	for _, f := range fields {
		if v, ok, err := parse(r, f.key); ok && err == nil {
			*f.dst = v
		}
	}
}

// parseInfoTime parses a timestamp of the info response. The server emits
// infoNoValue for a timestamp that is not set yet, which yields the zero time,
// as does any value that does not match the expected layout.
func parseInfoTime(val string) time.Time {
	if val == "" || val == infoNoValue {
		return time.Time{}
	}

	t, err := time.Parse(infoTimeLayout, val)
	if err != nil {
		return time.Time{}
	}

	return t
}

// earliestTime returns the earlier of two timestamps, ignoring a zero one.
func earliestTime(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}

	return a
}

// latestTime returns the later of two timestamps, ignoring a zero one.
func latestTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}

// firstKnownName returns current if it is a known name, see [knownName], and
// next otherwise, so that a merged status takes the job id and the namespace
// from the first row that is not a stub.
func firstKnownName(current, next string) string {
	if knownName(current) {
		return current
	}

	return next
}
