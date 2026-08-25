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
