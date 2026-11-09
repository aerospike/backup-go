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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// testMetadataPath is where the commit record of the test backup sits.
	testMetadataPath = testBackupID + "/" + metadataFile
	// testFallbackReason is why the test backup fell back to fuzzy.
	testFallbackReason = "change stream lost"
	// testPolicyBestEffort is the backup policy a test backup was taken with.
	testPolicyBestEffort = "best-effort"
)

// readTestCommit reads the commit record body holds, or the record of a backup
// that has none when body is nil.
func readTestCommit(t *testing.T, body []byte) CommitRecord {
	t.Helper()

	b := newTestBackup(testBackupID)
	if body != nil {
		b.put(testMetadataPath, body)
	}

	root := t.TempDir()
	writeBackup(t, root, b)

	record, err := newTestStreamer(t, &localStore{root: root}).readCommit(t.Context())
	require.NoError(t, err)

	return record
}

// nested returns a JSON value nested depth arrays deep.
func nested(depth int) string {
	return strings.Repeat("[", depth) + strings.Repeat("]", depth)
}

func TestReadCommit(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("s", maxStatusLen+1)

	tests := []struct {
		name      string
		give      []byte
		want      CommitRecord
		wantState CommitState
	}{
		{
			name: "strict backup",
			give: []byte(`{"status":"complete","achieved_consistency":"strict","backup_policy":"strict"}`),
			want: CommitRecord{
				Status: StatusComplete, AchievedConsistency: ConsistencyStrict, BackupPolicy: ConsistencyStrict,
			},
			wantState: CommitPresent,
		},
		{
			name: "best-effort backup that fell back",
			give: []byte(`{"status":"complete","achieved_consistency":"fuzzy",` +
				`"backup_policy":"best-effort","fallback_reason":"change stream lost","base_scan_end_ms":5}`),
			want: CommitRecord{
				Status: StatusComplete, AchievedConsistency: ConsistencyFuzzy,
				BackupPolicy: testPolicyBestEffort, FallbackReason: testFallbackReason,
			},
			wantState: CommitPresent,
		},
		{
			name:      "record written before the status",
			give:      []byte(`{"namespace":"source-ns1"}`),
			want:      CommitRecord{Namespace: testNS},
			wantState: CommitPresent,
		},
		{
			name:      "full backup",
			give:      []byte(`{"status":"complete","backup_type":"full"}`),
			want:      CommitRecord{Status: StatusComplete, BackupType: BackupTypeFull},
			wantState: CommitPresent,
		},
		{
			name: "incremental backup",
			give: []byte(`{"status":"complete","backup_type":"incremental",` +
				`"modified_after_unix_s":1700000000}`),
			want:      CommitRecord{Status: StatusComplete, BackupType: BackupTypeIncremental},
			wantState: CommitPresent,
		},
		{
			name:      "full backup with a bound",
			give:      []byte(`{"status":"complete","backup_type":"full","modified_before_unix_s":1700000000}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "incremental backup without a bound",
			give:      []byte(`{"status":"complete","backup_type":"incremental"}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name: "bound before the epoch of the server",
			give: []byte(`{"status":"complete","backup_type":"incremental",` +
				`"modified_after_unix_s":1262304000}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name: "bound that is not an integer",
			give: []byte(`{"status":"complete","backup_type":"incremental",` +
				`"modified_after_unix_s":1700000000.0}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "unknown backup type",
			give:      []byte(`{"status":"complete","backup_type":"differential"}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "escaped backslash before u0000",
			give:      []byte(`{"status":"complete","note":"\\u0000"}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "largest integer",
			give:      []byte(`{"status":"complete","checkpoint_ms":9223372036854775807}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "deepest nesting",
			give:      []byte(`{"status":"complete","deep":` + nested(maxJSONDepth-1) + `}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "status of another type",
			give:      []byte(`{"status":1,"achieved_consistency":null}`),
			want:      CommitRecord{Status: unreadableField, AchievedConsistency: unreadableField},
			wantState: CommitPresent,
		},
		{
			name:      "empty status",
			give:      []byte(`{"status":""}`),
			want:      CommitRecord{Status: unreadableField},
			wantState: CommitPresent,
		},
		{
			name:      "status too long",
			give:      []byte(`{"status":"` + tooLong + `"}`),
			want:      CommitRecord{Status: unreadableField},
			wantState: CommitPresent,
		},
		{
			name:      "longest status",
			give:      []byte(`{"status":"` + tooLong[1:] + `"}`),
			want:      CommitRecord{Status: tooLong[1:]},
			wantState: CommitPresent,
		},
		{
			name:      "fallback reason of another type",
			give:      []byte(`{"status":"complete","fallback_reason":7}`),
			want:      CommitRecord{Status: StatusComplete},
			wantState: CommitPresent,
		},
		{
			name:      "no commit record",
			wantState: CommitAbsent,
		},
		{
			name:      "not JSON",
			give:      []byte(`status=complete`),
			wantState: CommitCorrupt,
		},
		{
			name:      "trailing data",
			give:      []byte(`{"status":"complete"} {}`),
			wantState: CommitCorrupt,
		},
		{
			name:      "JSON array",
			give:      []byte(`[{"status":"complete"}]`),
			wantState: CommitCorrupt,
		},
		{
			name:      "JSON null",
			give:      []byte(`null`),
			wantState: CommitCorrupt,
		},
		{
			name:      "not UTF-8",
			give:      []byte("{\"status\":\"complete\xff\"}"),
			wantState: CommitCorrupt,
		},
		{
			name:      "string holding a NUL character",
			give:      []byte(`{"status":"complete","note":"a\u0000b"}`),
			wantState: CommitCorrupt,
		},
		{
			name:      "key holding a NUL character",
			give:      []byte(`{"status":"complete","\u0000":1}`),
			wantState: CommitCorrupt,
		},
		{
			name:      "integer beyond 64 bits",
			give:      []byte(`{"status":"complete","checkpoint_ms":9223372036854775808}`),
			wantState: CommitCorrupt,
		},
		{
			name:      "real number overflowing",
			give:      []byte(`{"status":"complete","ratio":1e999}`),
			wantState: CommitCorrupt,
		},
		{
			name:      "nested too deep",
			give:      []byte(`{"status":"complete","deep":` + nested(maxJSONDepth) + `}`),
			wantState: CommitCorrupt,
		},
		{
			name: "over the size limit",
			give: append([]byte(`{"status":"complete","pad":"`),
				append(bytes.Repeat([]byte{'x'}, maxMetadataSize), '"', '}')...),
			wantState: CommitCorrupt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := readTestCommit(t, tt.give)

			assert.Equal(t, tt.wantState, got.State)
			assert.Equal(t, testMetadataPath, got.Path)

			if tt.wantState == CommitCorrupt {
				require.Error(t, got.Err)
				return
			}

			require.NoError(t, got.Err)

			got.Path, got.State = "", 0
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestReadCommit_Unreadable(t *testing.T) {
	t.Parallel()

	b := newTestBackup(testBackupID)
	b.put(testMetadataPath, []byte(`{"status":"complete"}`))

	s := newFaultyStreamer(t, b, func(st *faultyStore) {
		st.failOpen = failingOn(metadataFile)
	})

	got, err := s.readCommit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, CommitUnreadable, got.State)
	require.ErrorIs(t, got.Err, errStorage)
	require.ErrorIs(t, got.Refusal(false), ErrCommitUnreadable)
}

func TestReadCommit_CanceledContext(t *testing.T) {
	t.Parallel()

	b := newTestBackup(testBackupID)
	root := t.TempDir()
	writeBackup(t, root, b)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := newTestStreamer(t, &localStore{root: root}).readCommit(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCommitRecordRefusal(t *testing.T) {
	t.Parallel()

	present := func(status, consistency string) CommitRecord {
		return CommitRecord{State: CommitPresent, Status: status, AchievedConsistency: consistency}
	}

	errBroken := errors.New("broken")

	tests := []struct {
		wantStrict error
		wantFuzzy  error
		name       string
		give       CommitRecord
	}{
		{name: "strict backup", give: present(StatusComplete, ConsistencyStrict)},
		{
			name:       "fuzzy backup",
			give:       present(StatusComplete, ConsistencyFuzzy),
			wantStrict: ErrBackupNotStrict,
		},
		{name: "complete backup written before the consistency", give: present(StatusComplete, "")},
		{name: "legacy record", give: present("", "")},
		{
			name:       "unreadable consistency",
			give:       present(StatusComplete, unreadableField),
			wantStrict: ErrBackupNotStrict,
		},
		{
			name:       "aborted backup",
			give:       present(StatusAborted, ConsistencyStrict),
			wantStrict: ErrBackupAborted,
			wantFuzzy:  ErrBackupAborted,
		},
		{
			name:       "aborted legacy backup",
			give:       present(StatusAborted, ""),
			wantStrict: ErrBackupAborted,
			wantFuzzy:  ErrBackupAborted,
		},
		{
			name:       "status missing beside the consistency",
			give:       present("", ConsistencyStrict),
			wantStrict: ErrBackupIncomplete,
			wantFuzzy:  ErrBackupIncomplete,
		},
		{
			name:       "unreadable status",
			give:       present(unreadableField, ""),
			wantStrict: ErrBackupIncomplete,
			wantFuzzy:  ErrBackupIncomplete,
		},
		{
			name:       "unknown status",
			give:       present("in_progress", ""),
			wantStrict: ErrBackupIncomplete,
			wantFuzzy:  ErrBackupIncomplete,
		},
		{
			name:       "absent",
			give:       CommitRecord{State: CommitAbsent},
			wantStrict: ErrCommitAbsent,
			wantFuzzy:  ErrCommitAbsent,
		},
		{
			name:       "corrupt",
			give:       CommitRecord{State: CommitCorrupt, Err: errBroken},
			wantStrict: ErrCommitCorrupt,
			wantFuzzy:  ErrCommitCorrupt,
		},
		{
			name:       "unreadable",
			give:       CommitRecord{State: CommitUnreadable, Err: errBroken},
			wantStrict: ErrCommitUnreadable,
			wantFuzzy:  ErrCommitUnreadable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for strict, want := range map[bool]error{true: tt.wantStrict, false: tt.wantFuzzy} {
				err := tt.give.Refusal(strict)
				if want == nil {
					require.NoError(t, err, "strict %v", strict)
					continue
				}

				require.ErrorIs(t, err, want, "strict %v", strict)
			}
		})
	}
}

func TestCommitStateString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "present", CommitPresent.String())
	assert.Equal(t, "absent", CommitAbsent.String())
	assert.Equal(t, "unreadable", CommitUnreadable.String())
	assert.Equal(t, "corrupt", CommitCorrupt.String())
	assert.Equal(t, "unknown", CommitState(0).String())
}

// A fuzzy backup is restored from its query stream alone, so its change stream
// is not validated as part of it, whichever way the backup is walked.
func TestStreaming_ChangeStreamOfAFuzzyBackupIsSkipped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		give       string
		wantChange bool
	}{
		{name: "fuzzy", give: ConsistencyFuzzy},
		{name: "strict", give: ConsistencyStrict, wantChange: true},
		{name: "written before the consistency", wantChange: true},
	}

	for _, tt := range tests {
		b := newTestBackupTree(t, 2, 2, 2, 2)

		body := `{"status":"complete"}`
		if tt.give != "" {
			body = `{"status":"complete","achieved_consistency":"` + tt.give + `"}`
		}

		b.put(testMetadataPath, []byte(body))

		for name, run := range streamRuns(t.Context(), 100) {
			t.Run(tt.name+"/"+name, func(t *testing.T) {
				t.Parallel()

				root := t.TempDir()
				writeBackup(t, root, b)
				s := newTestStreamer(t, &localStore{root: root})

				segments := collect(t, func(out chan<- Segment) error { return run(s, out) })

				change := 0

				for _, seg := range segments {
					if seg.Stream == ChangeStream {
						change++
					}
				}

				assert.Equal(t, tt.wantChange, change > 0)

				stats := s.Stats()
				assert.Equal(t, !tt.wantChange, stats.ChangeStreamSkipped)
				assert.Equal(t, CommitPresent, stats.Commit.State)
				assert.Equal(t, tt.give, stats.Commit.AchievedConsistency)
				assert.Equal(t, testMetadataPath, stats.Commit.Path)
			})
		}
	}
}
