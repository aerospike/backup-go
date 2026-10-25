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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aerospike/backup-go/errclass"
)

// Every backup ends with a commit record, <backup id>/metadata.json, written
// once the backup is over. A restore reads it before anything else and refuses
// to start unless it says the backup completed, so it decides whether a backup
// can be restored at all, however sound its segments are.
const (
	// metadataFile is the name of the commit record.
	metadataFile = "metadata.json"
	// maxMetadataSize is the largest commit record the server reads. A larger
	// one is corrupt.
	maxMetadataSize = 64 << 10

	// The longest values of the fields the server reads, one byte short of
	// the buffers it reads them into.
	maxStatusLen      = 31
	maxConsistencyLen = 15
	maxReasonLen      = 191
	maxNamespaceLen   = 31

	// maxJSONDepth is the deepest nesting the JSON parser of the server
	// accepts.
	maxJSONDepth = 2048
	// citrusleafEpochMillis is the Unix time, in milliseconds, of the epoch
	// the server keeps time in, 2010-01-01. No backup bound is set before it.
	citrusleafEpochMillis = 1262304000000

	// unreadableField is what the server reads a field as when it is present
	// but not a string, empty or too long. No check takes it for a valid
	// value.
	unreadableField = "?"
)

// The values of the fields of a commit record.
const (
	// StatusComplete is the status of a backup that ran to the end.
	StatusComplete = "complete"
	// StatusAborted is the status of a backup that was stopped.
	StatusAborted = "aborted"
	// ConsistencyStrict is what a backup achieved when its change stream
	// covers the whole time it ran.
	ConsistencyStrict = "strict"
	// ConsistencyFuzzy is what a backup achieved when it captured the scan
	// alone, without its change stream.
	ConsistencyFuzzy = "fuzzy"
	// BackupTypeFull is a backup of every record, taken without a
	// modification time bound.
	BackupTypeFull = "full"
	// BackupTypeIncremental is a backup of the records modified within a
	// time bound.
	BackupTypeIncremental = "incremental"
)

// Errors a restore refuses a backup with because of its commit record.
var (
	// ErrCommitAbsent is returned for a backup with no commit record, which
	// is what wrong coordinates or a backup that never finished look like.
	ErrCommitAbsent = fmt.Errorf("%w: no commit record", errclass.ErrNotFound)
	// ErrCommitUnreadable is returned for a commit record the storage could
	// not return.
	ErrCommitUnreadable = fmt.Errorf("%w: commit record could not be read", errclass.ErrStorage)
	// ErrCommitCorrupt is returned for a commit record over the size limit or
	// that is not a JSON object.
	ErrCommitCorrupt = fmt.Errorf("%w: commit record is not valid", errclass.ErrCorruptData)
	// ErrBackupAborted is returned for a backup its commit record says was
	// aborted.
	ErrBackupAborted = fmt.Errorf("%w: backup was aborted", errclass.ErrCorruptData)
	// ErrBackupIncomplete is returned for a backup its commit record does not
	// say completed.
	ErrBackupIncomplete = fmt.Errorf("%w: backup status is not complete", errclass.ErrCorruptData)
	// ErrBackupNotStrict is returned, under a strict restore, for a backup its
	// commit record does not say is strict.
	ErrBackupNotStrict = fmt.Errorf("%w: backup consistency is not strict", errclass.ErrUnsupported)
)

// CommitState is what reading the commit record of a backup established.
type CommitState int

const (
	// CommitPresent is a commit record that was read and parsed.
	CommitPresent CommitState = iota + 1
	// CommitAbsent is a commit record the storage confirmed is not there.
	CommitAbsent
	// CommitUnreadable is a commit record whose presence could not be
	// established, because the storage failed to return it.
	CommitUnreadable
	// CommitCorrupt is a commit record over the size limit or that is not a
	// JSON object. Reading it again would read the same.
	CommitCorrupt
)

// String returns the name of the state.
func (s CommitState) String() string {
	switch s {
	case CommitPresent:
		return "present"
	case CommitAbsent:
		return "absent"
	case CommitUnreadable:
		return "unreadable"
	case CommitCorrupt:
		return "corrupt"
	default:
		return "unknown"
	}
}

// CommitRecord is what the commit record of a backup says, read the way the
// server reads it before a restore.
//
// A field the record lacks is empty. Status and AchievedConsistency, the two
// fields a restore decides on, read as "?" when present but not a string,
// empty, or too long.
type CommitRecord struct {
	// Err is why the record could not be used. It is set for
	// CommitUnreadable and CommitCorrupt.
	Err error
	// Path locates the record in the storage.
	Path string
	// Status is StatusComplete or StatusAborted, or empty for a record
	// written before the field.
	Status string
	// BackupPolicy is the consistency the backup was asked for: "strict",
	// "best-effort" or "fuzzy".
	BackupPolicy string
	// AchievedConsistency is what the backup achieved, ConsistencyStrict or
	// ConsistencyFuzzy, or empty for a record written before the field.
	AchievedConsistency string
	// FallbackReason is why a best-effort backup fell back to fuzzy.
	FallbackReason string
	// Namespace is the namespace the backup was taken of.
	Namespace string
	// BackupType is BackupTypeFull or BackupTypeIncremental, or empty when
	// the record lacks it, names another type, or names a type its own
	// modification time bounds contradict: a full backup has none, an
	// incremental one has at least one.
	BackupType string
	// State is what reading the record established.
	State CommitState
}

// Fuzzy reports whether the record says the backup is fuzzy. A fuzzy backup is
// restored from its query stream alone: its change stream is never read.
func (c *CommitRecord) Fuzzy() bool {
	return c.State == CommitPresent && c.AchievedConsistency == ConsistencyFuzzy
}

// Refusal returns why a restore of the backup would refuse to start, or nil
// when it would start. strict is what a restore with restore-policy strict
// does, which also refuses a backup that is not shown to be strict.
//
// A restore needs a record it can read: an absent, unreadable or corrupt one
// is refused, because nothing shows the backup complete. So is any status
// other than complete. An empty status passes only for a record written before
// both the status and the consistency fields.
func (c *CommitRecord) Refusal(strict bool) error {
	switch c.State {
	case CommitPresent:
	case CommitAbsent:
		return fmt.Errorf("%w: %s", ErrCommitAbsent, c.Path)
	case CommitCorrupt:
		return fmt.Errorf("%w: %s: %w", ErrCommitCorrupt, c.Path, c.Err)
	default:
		return fmt.Errorf("%w: %s: %w", ErrCommitUnreadable, c.Path, c.Err)
	}

	if c.Status == StatusAborted {
		return ErrBackupAborted
	}

	if c.Status != StatusComplete && (c.Status != "" || c.AchievedConsistency != "") {
		return fmt.Errorf("%w: %q", ErrBackupIncomplete, c.Status)
	}

	if strict && c.AchievedConsistency != "" && c.AchievedConsistency != ConsistencyStrict {
		return fmt.Errorf("%w: %q", ErrBackupNotStrict, c.AchievedConsistency)
	}

	return nil
}

// readCommit reads the commit record of the backup. Anything wrong with the
// record is described by what is returned; only a canceled context is an error.
func (s *Streamer) readCommit(ctx context.Context) (CommitRecord, error) {
	record := CommitRecord{Path: path.Join(s.backupID, metadataFile)}

	data, err := s.readMetadata(ctx, record.Path)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return record, ctxErr
	}

	record.describe(data, err)

	return record, nil
}

// describe fills the record in from what reading it returned: its body, or why
// there is none.
func (c *CommitRecord) describe(data []byte, readErr error) {
	switch {
	case errors.Is(readErr, ErrSegmentMissing):
		c.State = CommitAbsent
	case readErr != nil:
		c.State = CommitUnreadable
		c.Err = readErr
	default:
		if err := parseCommit(data, c); err != nil {
			c.State = CommitCorrupt
			c.Err = err

			return
		}

		c.State = CommitPresent
	}
}

// readMetadata downloads the commit record, refusing one larger than the server
// reads.
func (s *Streamer) readMetadata(ctx context.Context, metadataPath string) ([]byte, error) {
	body, err := s.store.open(ctx, metadataPath)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	data, err := io.ReadAll(io.LimitReader(body, maxMetadataSize+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", metadataPath, err)
	}

	return data, nil
}

// parseCommit parses a commit record into record, refusing what the server
// refuses: a record over the size limit, one that does not parse, and one that
// parses into anything but a JSON object.
func parseCommit(data []byte, record *CommitRecord) error {
	if len(data) > maxMetadataSize {
		return fmt.Errorf("over %d bytes", maxMetadataSize)
	}

	// The server parses JSON as strict UTF-8.
	if !utf8.Valid(data) {
		return errors.New("not valid UTF-8")
	}

	if !json.Valid(data) {
		return errors.New("not valid JSON")
	}

	if err := strictJSON(data); err != nil {
		return err
	}

	var fields map[string]json.RawMessage

	// Unmarshal leaves the map nil for a bare null.
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return errors.New("not a JSON object")
	}

	record.Status = nameOrUnreadable(fields, "status", maxStatusLen)
	record.AchievedConsistency = nameOrUnreadable(fields, "achieved_consistency", maxConsistencyLen)
	record.FallbackReason = name(fields, "fallback_reason", maxReasonLen)
	record.BackupPolicy = name(fields, "backup_policy", maxMetadataSize)
	record.Namespace = name(fields, "namespace", maxNamespaceLen)
	record.BackupType = backupType(fields)

	return nil
}

// backupType reads the type of the backup and checks it against the bounds it
// is derived from: a full backup has no modification time bound, and an
// incremental one has one. A type the bounds contradict, or bounds that cannot
// be read, leave the type unknown.
func backupType(fields map[string]json.RawMessage) string {
	after, ok := modifiedBound(fields, "modified_after_unix_s")
	if !ok {
		return ""
	}

	before, ok := modifiedBound(fields, "modified_before_unix_s")
	if !ok {
		return ""
	}

	bounded := after || before

	switch name(fields, "backup_type", maxMetadataSize) {
	case BackupTypeFull:
		if !bounded {
			return BackupTypeFull
		}
	case BackupTypeIncremental:
		if bounded {
			return BackupTypeIncremental
		}
	}

	return ""
}

// modifiedBound reports whether a modification time bound is set. An absent
// bound is unset. ok is false for a bound that is not an integer number of
// seconds after the epoch of the server, which no backup writes.
func modifiedBound(fields map[string]json.RawMessage, key string) (set, ok bool) {
	raw, present := fields[key]
	if !present {
		return false, true
	}

	raw = bytes.TrimSpace(raw)
	if bytes.ContainsAny(raw, ".eE") {
		return false, false
	}

	seconds, err := strconv.ParseInt(string(raw), decimalBase, 64)
	if err != nil || seconds <= 0 || uint64(seconds) > math.MaxUint64/1000 ||
		uint64(seconds)*1000 <= citrusleafEpochMillis {
		return false, false
	}

	return true, true
}

// strictJSON refuses what the JSON parser of the server refuses in a document
// that encoding/json accepts: a string holding a NUL character, an integer that
// does not fit 64 bits, a real number too large to be one, and nesting deeper
// than maxJSONDepth. data must be valid JSON.
func strictJSON(data []byte) error {
	depth := 0

	for i := 0; i < len(data); i++ {
		switch c := data[i]; {
		case c == '"':
			end, err := skipJSONString(data, i)
			if err != nil {
				return err
			}

			i = end
		case c == '{' || c == '[':
			depth++
			if depth > maxJSONDepth {
				return fmt.Errorf("nested deeper than %d", maxJSONDepth)
			}
		case c == '}' || c == ']':
			depth--
		case c == '-' || (c >= '0' && c <= '9'):
			end := i
			for end < len(data) && strings.IndexByte("+-.0123456789eE", data[end]) >= 0 {
				end++
			}

			if err := checkJSONNumber(string(data[i:end])); err != nil {
				return err
			}

			i = end - 1
		}
	}

	return nil
}

// skipJSONString returns the offset of the quote closing the string opened at
// data[start], refusing a string holding an escaped NUL character.
func skipJSONString(data []byte, start int) (int, error) {
	for i := start + 1; i < len(data); i++ {
		switch data[i] {
		case '"':
			return i, nil
		case '\\':
			if bytes.HasPrefix(data[i+1:], []byte("u0000")) {
				return 0, errors.New("string holds a NUL character")
			}

			// The character after the backslash is escaped, whatever it is.
			i++
		}
	}

	return len(data), nil
}

// checkJSONNumber refuses a number the server cannot hold: an integer outside
// 64 bits, or a real number that overflows a double.
func checkJSONNumber(number string) error {
	if !strings.ContainsAny(number, ".eE") {
		if _, err := strconv.ParseInt(number, decimalBase, 64); err != nil {
			return fmt.Errorf("integer %s does not fit 64 bits", number)
		}

		return nil
	}

	if f, _ := strconv.ParseFloat(number, 64); math.IsInf(f, 0) {
		return fmt.Errorf("real number %s overflows", number)
	}

	return nil
}

// name reads a string field. A field that is absent, not a string, or longer
// than maxLen reads as empty, never as a truncation that could pass for a
// shorter value.
func name(fields map[string]json.RawMessage, key string, maxLen int) string {
	raw, ok := fields[key]
	if !ok || !bytes.HasPrefix(raw, []byte{'"'}) {
		return ""
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil || len(s) > maxLen {
		return ""
	}

	return s
}

// nameOrUnreadable reads a string field as name does, except that a field that
// is present and not a value it can read, empty included, reads as "?".
func nameOrUnreadable(fields map[string]json.RawMessage, key string, maxLen int) string {
	s := name(fields, key, maxLen)

	if _, present := fields[key]; s == "" && present {
		return unreadableField
	}

	return s
}
