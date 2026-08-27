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

// UnknownRecordIndex marks an issue that does not point at a single record.
const UnknownRecordIndex = -1

// ValidationReport is the outcome of a validation run. The segments it checked
// were parsed record by record, and the manifests it checked were compared
// against the storage, so a report without issues says both that the data reads
// back and that the backup is not missing pieces it claims to have.
type ValidationReport struct {
	// BackupID identifies the validated backup.
	BackupID string
	// Issues describes the segments that failed to parse. A run over a backup
	// that is broken from end to end would produce one issue per segment, so
	// the list is capped: InvalidSegments, not its length, is the number of
	// failures.
	Issues []ValidationIssue
	// Manifests is what comparing manifests against the storage found.
	Manifests ManifestReport
	// TotalSegments is the number of segments the run was given to check. A
	// sampled run never enumerates the backup, so this is the size of the
	// sample and not the size of the backup.
	TotalSegments int64
	// CheckedSegments is the number of segments actually parsed.
	CheckedSegments int64
	// ValidSegments is the number of segments parsed without any error.
	ValidSegments int64
	// InvalidSegments is the number of segments that failed to parse.
	InvalidSegments int64
	// TotalRecords is the number of records read across all checked segments.
	TotalRecords int64
	// TotalBytes is the number of segment bytes parsed, tail slack excluded.
	TotalBytes int64
	// SkippedCompressed is the number of compressed records that were walked
	// over without being decoded.
	SkippedCompressed int64
}

// Failed reports whether the run found anything wrong with the backup.
func (r *ValidationReport) Failed() bool {
	return r.InvalidSegments > 0 || r.Manifests.Problems > 0
}

// Truncated reports whether more segments failed than Issues describes.
func (r *ValidationReport) Truncated() bool {
	return r.InvalidSegments > int64(len(r.Issues))
}

// ValidationIssue describes a segment that failed validation, either because a
// record did not parse or because the segment could not be read at all.
type ValidationIssue struct {
	// Err is the reason the segment failed.
	Err error
	// Namespace is the namespace the segment belongs to.
	Namespace string
	// SegmentPath locates the segment in the storage it was read from.
	SegmentPath string
	// RecordIndex is the position of the offending record inside the segment,
	// or UnknownRecordIndex when the failure is not tied to a single record.
	RecordIndex int
	// Offset is the byte offset of the offending record inside the segment.
	Offset int
}

// ManifestReport is what checking manifests against the storage found. A
// manifest lists the segments a partition wrote and how big they are, so it is
// the only thing that can tell whether a segment is missing rather than merely
// unreadable. A sampled run draws its segments from the manifests and so checks
// them on the way; a run over every segment lists the data directories and does
// not read manifests at all.
type ManifestReport struct {
	// Issues describes the manifests that did not match the storage. Like the
	// segment issues it is capped, and Problems counts them all.
	Issues []ManifestIssue
	// Total is the number of manifests the run listed.
	Total int64
	// Checked is the number of manifests actually read.
	Checked int64
	// CheckedSegments is the number of checked segments a manifest named,
	// which are the ones whose size could be compared against a record of it.
	CheckedSegments int64
	// MissingSegments is the number of recorded segments the storage does not
	// hold at all.
	MissingSegments int64
	// Unrecorded is the number of checked segments that no manifest names.
	// They were read like the others, but nothing recorded what they should
	// be: they are either leftovers of an interrupted flush or the sign of a
	// manifest that never made it. They are reported rather than counted as
	// problems, because a backup can hold them and still restore.
	Unrecorded int64
	// UnrecordedExamples names some of them, capped like the issue lists.
	UnrecordedExamples []string
	// Problems is the total number of things wrong: unreadable manifests,
	// missing segments and segments whose size does not match the record.
	Problems int64
}

// Truncated reports whether more problems were found than Issues describes.
func (r *ManifestReport) Truncated() bool {
	return r.Problems > int64(len(r.Issues))
}

// ManifestIssue describes a mismatch between a manifest and the storage.
type ManifestIssue struct {
	// Err is what did not match.
	Err error
	// Namespace is the namespace the manifest belongs to.
	Namespace string
	// ManifestPath locates the manifest.
	ManifestPath string
	// SegmentPath locates the recorded segment that is missing or the wrong
	// size. It is empty when the manifest itself could not be read.
	SegmentPath string
}
