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

package segvalidator

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/streamers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubChangeSegmentPath names the nth change stream segment of the stub node.
func stubChangeSegmentPath(n int) string {
	return fmt.Sprintf("%s/ns/%s/change-stream/BB951D8A16DC7A2/data/s%d.seg", stubBackupID, stubNS, n)
}

// The server refuses a segment whose frame names another stream than the one
// whose manifest listed it.
func TestSegValidator_BlockTypeOfTheStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		giveHex    string
		giveStream streamers.Stream
	}{
		{name: "query segment in the query stream", giveHex: fixtureSegmentHex, giveStream: streamers.QueryStream},
		{
			name:       "change segment in the change stream",
			giveHex:    fixtureChangeSegmentHex,
			giveStream: streamers.ChangeStream,
		},
		{
			name:       "query segment in the change stream",
			giveHex:    fixtureSegmentHex,
			giveStream: streamers.ChangeStream,
			wantErr:    segment.ErrBlockTypeMismatch,
		},
		{
			name:       "change segment in the query stream",
			giveHex:    fixtureChangeSegmentHex,
			giveStream: streamers.QueryStream,
			wantErr:    segment.ErrBlockTypeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			streamer := newStubStreamer(decodeSegmentHex(t, tt.giveHex), 1)
			streamer.streamKind = tt.giveStream

			if tt.giveStream == streamers.ChangeStream {
				streamer.pathOf = stubChangeSegmentPath
			}

			report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
			require.NoError(t, err)

			if tt.wantErr == nil {
				assert.False(t, report.Failed(), "issues: %v", report.Issues)
				return
			}

			require.Len(t, report.Issues, 1)
			require.ErrorIs(t, report.Issues[0].Err, tt.wantErr)
			assert.EqualValues(t, 1, report.InvalidSegments)
		})
	}
}

// The commit record decides whether a restore starts at all, and which restore
// the backup is validated for.
func TestSegValidator_CommitRecord(t *testing.T) {
	t.Parallel()

	const metadataPath = stubBackupID + "/metadata.json"

	present := func(status, consistency string) streamers.CommitRecord {
		return streamers.CommitRecord{
			State:               streamers.CommitPresent,
			Path:                metadataPath,
			Status:              status,
			AchievedConsistency: consistency,
		}
	}

	tests := []struct {
		wantRefusal error
		name        string
		give        streamers.CommitRecord
		wantState   string
		giveSkipped bool
		wantStrict  bool
	}{
		{
			name:       "strict backup",
			give:       present(streamers.StatusComplete, streamers.ConsistencyStrict),
			wantState:  "present",
			wantStrict: true,
		},
		{
			name:        "fuzzy backup is validated for a fuzzy restore",
			give:        present(streamers.StatusComplete, streamers.ConsistencyFuzzy),
			giveSkipped: true,
			wantState:   "present",
		},
		{
			name:       "backup written before the consistency",
			give:       present(streamers.StatusComplete, ""),
			wantState:  "present",
			wantStrict: true,
		},
		{
			name:        "unreadable consistency",
			give:        present(streamers.StatusComplete, "?"),
			wantState:   "present",
			wantStrict:  true,
			wantRefusal: streamers.ErrBackupNotStrict,
		},
		{
			name:        "aborted backup",
			give:        present(streamers.StatusAborted, streamers.ConsistencyStrict),
			wantState:   "present",
			wantStrict:  true,
			wantRefusal: streamers.ErrBackupAborted,
		},
		{
			name:        "no commit record",
			give:        streamers.CommitRecord{State: streamers.CommitAbsent, Path: metadataPath},
			wantState:   "absent",
			wantStrict:  true,
			wantRefusal: streamers.ErrCommitAbsent,
		},
		{
			name: "record not read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
			streamer.stats = streamers.Stats{Commit: tt.give, ChangeStreamSkipped: tt.giveSkipped}

			report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
			require.NoError(t, err)

			commit := report.Commit

			assert.Equal(t, tt.wantState, commit.State)
			assert.Equal(t, tt.wantStrict, commit.Strict)
			assert.Equal(t, tt.giveSkipped, commit.ChangeStreamSkipped)
			assert.Equal(t, tt.give.Path, commit.Path)
			assert.Equal(t, tt.give.Status, commit.Status)
			assert.Equal(t, tt.give.AchievedConsistency, commit.AchievedConsistency)
			assert.Zero(t, report.InvalidSegments)

			if tt.wantRefusal == nil {
				require.NoError(t, commit.Refusal)
				assert.False(t, report.Failed())

				return
			}

			require.ErrorIs(t, commit.Refusal, tt.wantRefusal)
			assert.True(t, report.Failed())
		})
	}
}

func TestSegValidator_SupersededManifestsAreReported(t *testing.T) {
	t.Parallel()

	const superseded = 2

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.stats = streamers.Stats{ManifestsSuperseded: superseded}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	assert.EqualValues(t, superseded, report.Manifests.Superseded)
	assert.False(t, report.Failed())
}

// A commit record naming a namespace the storage does not hold is a backup a
// restore refuses, which is reported rather than taken for a backup id that does
// not exist.
func TestSegValidator_CommitRecordWithoutData(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, stubBackupID), 0o750))

	metadata := `{"status":"complete","namespace":"` + stubNS + `","backup_type":"full"}`
	require.NoError(t, os.WriteFile(filepath.Join(root, stubBackupID, "metadata.json"), []byte(metadata), 0o600))

	streamer, err := streamers.NewLocal(root, stubBackupID)
	require.NoError(t, err)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	assert.True(t, report.Failed())
	assert.Equal(t, []string{stubNS}, report.EmptyNamespaces)
	assert.Equal(t, streamers.BackupTypeFull, report.Commit.BackupType)
	assert.Equal(t, stubNS, report.Commit.Namespace)
	require.NoError(t, report.Commit.Refusal)
	require.Len(t, report.Manifests.Issues, 1)
	require.ErrorIs(t, report.Manifests.Issues[0].Err, streamers.ErrNoQueryManifests)
}

// A backup id nothing was written under is not a backup at all.
func TestSegValidator_NothingAtAll(t *testing.T) {
	t.Parallel()

	streamer, err := streamers.NewLocal(t.TempDir(), stubBackupID)
	require.NoError(t, err)

	_, err = newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.ErrorIs(t, err, ErrNoSegments)
}
