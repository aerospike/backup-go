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
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testOtherNS is a namespace the test backup does not hold.
const testOtherNS = "other-ns"

// commitBody is a commit record of a complete backup of namespace.
func commitBody(namespace, backupType string) []byte {
	bound := ""
	if backupType == BackupTypeIncremental {
		bound = `,"modified_after_unix_s":1700000000`
	}

	return []byte(`{"status":"complete","namespace":"` + namespace + `","backup_type":"` +
		backupType + `"` + bound + `}`)
}

// The server restores a partition from a bounded number of manifests and fails
// the restore of a partition that has more.
func TestStreamAll_PartitionWithTooManyManifests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		giveExtra int
		wantIssue bool
	}{
		{name: "as many as the server restores", giveExtra: maxManifestsPerPartition - 1},
		{name: "one more than the server restores", giveExtra: maxManifestsPerPartition, wantIssue: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackupTree(t, 2, 1, 0, 0)
			b.put(testMetadataPath, commitBody(testNS, BackupTypeFull))

			root := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), manifestDir)
			for i := range tt.giveExtra {
				b.put(path.Join(root, fmt.Sprintf(testManifestName, 0, 0x100+i)), testManifest(testNS, 0, nil))
			}

			dir := t.TempDir()
			writeBackup(t, dir, b)
			s := newTestStreamer(t, &localStore{root: dir})

			collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

			stats := s.Stats()
			if !tt.wantIssue {
				assert.Empty(t, stats.ManifestIssues)
				return
			}

			require.Len(t, stats.ManifestIssues, 1)
			require.ErrorIs(t, stats.ManifestIssues[0].Err, ErrManifestUnusable)
		})
	}
}

// A namespace a restore finds no query stream manifest for fails the restore,
// unless the backup is incremental and so restorable as part of a list.
func TestStreamAll_EmptyNamespaces(t *testing.T) {
	t.Parallel()

	queryDir := func(namespace string) string {
		return path.Join(testBackupID, namespacesDir, namespace, string(QueryStream), manifestDir)
	}

	tests := []struct {
		name          string
		giveCommit    []byte
		givePartition int
		wantEmpty     []string
		wantIssues    []string
	}{
		{
			name:          "full backup in order",
			giveCommit:    commitBody(testNS, BackupTypeFull),
			givePartition: 1,
		},
		{
			name:       "full backup of a namespace without query manifests",
			giveCommit: commitBody(testNS, BackupTypeFull),
			wantEmpty:  []string{testNS},
			wantIssues: []string{queryDir(testNS)},
		},
		{
			name:       "incremental backup of a namespace without query manifests",
			giveCommit: commitBody(testNS, BackupTypeIncremental),
			wantEmpty:  []string{testNS},
		},
		{
			name:          "commit record naming a namespace the storage does not hold",
			giveCommit:    commitBody(testOtherNS, BackupTypeFull),
			givePartition: 1,
			wantEmpty:     []string{testOtherNS},
			wantIssues:    []string{queryDir(testOtherNS)},
		},
		{
			name:       "no commit record",
			wantEmpty:  []string{testNS},
			wantIssues: []string{queryDir(testNS)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackupTree(t, tt.givePartition, 1, 1, 1)
			if tt.giveCommit != nil {
				b.put(testMetadataPath, tt.giveCommit)
			}

			dir := t.TempDir()
			writeBackup(t, dir, b)
			s := newTestStreamer(t, &localStore{root: dir})

			collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

			stats := s.Stats()
			assert.Equal(t, tt.wantEmpty, stats.EmptyNamespaces)

			issues := make([]string, 0, len(stats.ManifestIssues))

			for _, issue := range stats.ManifestIssues {
				require.ErrorIs(t, issue.Err, ErrNoQueryManifests)
				issues = append(issues, issue.Path)
			}

			assert.ElementsMatch(t, tt.wantIssues, issues)
		})
	}
}

// A sample does not list the manifests in full, so it cannot tell a namespace
// without them.
func TestStreamSample_EmptyNamespacesAreNotDetermined(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 0, 0, 1, 1)
	b.put(testMetadataPath, commitBody(testNS, BackupTypeFull))

	dir := t.TempDir()
	writeBackup(t, dir, b)
	s := newTestStreamer(t, &localStore{root: dir})

	collect(t, func(out chan<- Segment) error { return s.StreamSample(t.Context(), 10, out) })

	stats := s.Stats()
	assert.Empty(t, stats.EmptyNamespaces)
	assert.Empty(t, stats.ManifestIssues)
}
