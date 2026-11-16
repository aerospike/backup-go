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
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalValidation(t *testing.T) {
	t.Parallel()

	_, err := NewLocal("", testBackupID)
	require.Error(t, err, "NewLocal() with no root succeeded")

	_, err = NewLocal(t.TempDir(), "")
	require.Error(t, err, "NewLocal() with no backup id succeeded")

	s, err := NewLocal(t.TempDir(), testBackupID)
	require.NoError(t, err)

	assert.Equal(t, testBackupID, s.BackupID())
}

func TestLocalStore_MissingDirectoryHoldsNothing(t *testing.T) {
	t.Parallel()

	st := &localStore{root: t.TempDir()}

	dirs, err := listDirs(t.Context(), st, "no/such/directory")
	require.NoError(t, err)

	assert.Empty(t, dirs)

	seen := 0

	err = st.listFiles(t.Context(), "no/such/directory", func(file) error {
		seen++

		return nil
	})
	require.NoError(t, err)

	assert.Zero(t, seen, "listFiles() saw files, want none")
}

func TestLocalStore_ListFilesStops(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 4, 4, 0, 0)
	root := t.TempDir()
	writeBackup(t, root, b)

	st := &localStore{root: root}
	seen := 0

	err := st.listFiles(t.Context(), testBackupID, func(file) error {
		seen++

		return errStopListing
	})
	require.NoError(t, err)

	assert.Equal(t, 1, seen, "files seen after stopping the listing")
}

func TestLocalStore_RefusesToLeaveTheRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := filepath.Join(filepath.Dir(root), "outside.seg")

	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))

	st := &localStore{root: root}

	// A manifest naming a segment outside the backup must not make a validator
	// read it, whatever it points at.
	for _, p := range []string{"../outside.seg", "/etc/passwd"} {
		_, err := st.open(t.Context(), p)
		if assert.Error(t, err, "open(%q) succeeded, want it refused", p) {
			assert.NotErrorIs(t, err, ErrSegmentMissing,
				"open(%q) reported a missing segment, want the path refused", p)
		}
	}
}

func TestLocalStore_MissingFile(t *testing.T) {
	t.Parallel()

	st := &localStore{root: t.TempDir()}

	_, err := st.open(t.Context(), path.Join(testBackupID, "no-such-segment.seg"))
	require.ErrorIs(t, err, ErrSegmentMissing)
}

// writeBackup materializes a backup into a directory.
func writeBackup(t *testing.T, root string, b *testBackup) {
	t.Helper()

	for p, body := range b.files {
		full := filepath.Join(root, filepath.FromSlash(p))

		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750), "create directory")
		require.NoError(t, os.WriteFile(full, body, 0o600), "write file")
	}
}

func TestLocalStore_PathOutsideTheRoot(t *testing.T) {
	t.Parallel()

	st := &localStore{root: t.TempDir()}

	// A backup lives under the root directory, and nothing that would leave
	// it is read, listed or resolved.
	outside := "../elsewhere"

	require.Error(t, st.listLevel(t.Context(), outside, func(levelEntry) error { return nil }),
		"listLevel() outside the root succeeded")

	require.Error(t, st.listFiles(t.Context(), outside, func(file) error { return nil }),
		"listFiles() outside the root succeeded")

	_, err := st.open(t.Context(), outside)
	assert.Error(t, err, "open() outside the root succeeded")
}

func TestLocalStore_DirectoryThatIsAFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "notadir"), []byte("x"), 0o600))

	st := &localStore{root: root}

	// A path that exists but is not a directory is a failure, unlike a
	// directory that is simply not there.
	require.Error(t, st.listLevel(t.Context(), "notadir", func(levelEntry) error { return nil }),
		"listLevel() of a file succeeded")

	// The same goes for reading through it.
	_, err := st.open(t.Context(), "notadir/below")
	if assert.Error(t, err, "open() through a file succeeded") {
		assert.NotErrorIs(t, err, ErrSegmentMissing, "want a failure and not a missing segment")
	}
}

func TestLocalStore_ListingStopsOnTheCaller(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeBackup(t, root, newTestBackupTree(t, 2, 2, 0, 0))

	st := &localStore{root: root}
	dir := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir)

	seen := 0

	// A caller that has seen enough ends the listing without failing it.
	err := st.listLevel(t.Context(), dir, func(levelEntry) error {
		seen++

		return errStopListing
	})
	require.NoError(t, err, "want the listing to stop without failing")

	assert.Equal(t, 1, seen, "listLevel() handed over more entries after the first one stopped it")

	// A caller that fails is a failure of the listing.
	errCaller := errors.New("caller failed")

	err = st.listLevel(t.Context(), dir, func(levelEntry) error {
		return errCaller
	})
	require.ErrorIs(t, err, errCaller, "listLevel() want the failure of the caller")

	err = st.listFiles(t.Context(), dir, func(file) error {
		return errCaller
	})
	assert.ErrorIs(t, err, errCaller, "listFiles() want the failure of the caller")
}

func TestLocalStore_StoragePathOfAnUnrelatedFile(t *testing.T) {
	t.Parallel()

	// A path that cannot be expressed relative to the root is handed back as
	// it is rather than being turned into nonsense.
	st := &localStore{root: filepath.Join("relative", "root")}

	require.NotEmpty(t, st.storagePath(filepath.Join(string(filepath.Separator), "elsewhere", "s.seg")),
		"storagePath() want the path it was given")
}
