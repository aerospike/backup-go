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
)

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

// firstSet returns a if it is set, b otherwise.
func firstSet[T comparable](a, b T) T {
	var zero T
	if a != zero {
		return a
	}

	return b
}
