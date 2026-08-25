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

package streamers

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func TestNewLocalValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewLocal("", testBackupID); err == nil {
		t.Error("NewLocal() with no root succeeded, want an error")
	}

	if _, err := NewLocal(t.TempDir(), ""); err == nil {
		t.Error("NewLocal() with no backup id succeeded, want an error")
	}

	s, err := NewLocal(t.TempDir(), testBackupID)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}

	if s.BackupID() != testBackupID {
		t.Errorf("BackupID() = %q, want %q", s.BackupID(), testBackupID)
	}
}

func TestLocalStore_MissingDirectoryHoldsNothing(t *testing.T) {
	t.Parallel()

	st := &localStore{root: t.TempDir()}

	dirs, err := listDirs(t.Context(), st, "no/such/directory")
	if err != nil {
		t.Fatalf("listDirs() error = %v", err)
	}

	if len(dirs) != 0 {
		t.Errorf("listDirs() = %v, want nothing", dirs)
	}

	seen := 0

	err = st.listFiles(t.Context(), "no/such/directory", func(file) error {
		seen++

		return nil
	})
	if err != nil {
		t.Fatalf("listFiles() error = %v", err)
	}

	if seen != 0 {
		t.Errorf("listFiles() saw %d files, want none", seen)
	}
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
	if err != nil {
		t.Fatalf("listFiles() error = %v", err)
	}

	if seen != 1 {
		t.Errorf("saw %d files after stopping the listing, want 1", seen)
	}
}

func TestLocalStore_RefusesToLeaveTheRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := filepath.Join(filepath.Dir(root), "outside.seg")

	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	st := &localStore{root: root}

	// A manifest naming a segment outside the backup must not make a validator
	// read it, whatever it points at.
	for _, p := range []string{"../outside.seg", "/etc/passwd"} {
		if _, err := st.open(t.Context(), p); err == nil {
			t.Errorf("open(%q) succeeded, want it refused", p)
		} else if errors.Is(err, ErrSegmentMissing) {
			t.Errorf("open(%q) reported a missing segment, want the path refused", p)
		}
	}
}

func TestLocalStore_MissingFile(t *testing.T) {
	t.Parallel()

	st := &localStore{root: t.TempDir()}

	if _, err := st.open(t.Context(), path.Join(testBackupID, "no-such-segment.seg")); !errors.Is(err, ErrSegmentMissing) {
		t.Fatalf("open() error = %v, want ErrSegmentMissing", err)
	}
}

// writeBackup materializes a backup into a directory.
func writeBackup(t *testing.T, root string, b *testBackup) {
	t.Helper()

	for p, body := range b.files {
		full := filepath.Join(root, filepath.FromSlash(p))

		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatalf("create directory: %v", err)
		}

		if err := os.WriteFile(full, body, 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
}

func TestLocalStore_PathOutsideTheRoot(t *testing.T) {
	t.Parallel()

	st := &localStore{root: t.TempDir()}

	// A backup lives under the root directory, and nothing that would leave
	// it is read, listed or resolved.
	outside := "../elsewhere"

	if err := st.listLevel(t.Context(), outside, func(levelEntry) error { return nil }); err == nil {
		t.Error("listLevel() outside the root succeeded, want an error")
	}

	if err := st.listFiles(t.Context(), outside, func(file) error { return nil }); err == nil {
		t.Error("listFiles() outside the root succeeded, want an error")
	}

	if _, err := st.open(t.Context(), outside); err == nil {
		t.Error("open() outside the root succeeded, want an error")
	}
}

func TestLocalStore_DirectoryThatIsAFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "notadir"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	st := &localStore{root: root}

	// A path that exists but is not a directory is a failure, unlike a
	// directory that is simply not there.
	if err := st.listLevel(t.Context(), "notadir", func(levelEntry) error { return nil }); err == nil {
		t.Error("listLevel() of a file succeeded, want an error")
	}

	// The same goes for reading through it.
	if _, err := st.open(t.Context(), "notadir/below"); err == nil {
		t.Error("open() through a file succeeded, want an error")
	} else if errors.Is(err, ErrSegmentMissing) {
		t.Errorf("open() error = %v, want a failure and not a missing segment", err)
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
	if err != nil {
		t.Fatalf("listLevel() error = %v, want the listing to stop without failing", err)
	}

	if seen != 1 {
		t.Errorf("listLevel() handed over %d entries after the first one stopped it, want 1", seen)
	}

	// A caller that fails is a failure of the listing.
	errCaller := errors.New("caller failed")

	if err := st.listLevel(t.Context(), dir, func(levelEntry) error {
		return errCaller
	}); !errors.Is(err, errCaller) {
		t.Errorf("listLevel() error = %v, want the failure of the caller", err)
	}

	if err := st.listFiles(t.Context(), dir, func(file) error {
		return errCaller
	}); !errors.Is(err, errCaller) {
		t.Errorf("listFiles() error = %v, want the failure of the caller", err)
	}
}

func TestLocalStore_StoragePathOfAnUnrelatedFile(t *testing.T) {
	t.Parallel()

	// A path that cannot be expressed relative to the root is handed back as
	// it is rather than being turned into nonsense.
	st := &localStore{root: filepath.Join("relative", "root")}

	if got := st.storagePath(filepath.Join(string(filepath.Separator), "elsewhere", "s.seg")); got == "" {
		t.Fatal("storagePath() = \"\", want the path it was given")
	}
}
