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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
)

// NewLocal creates a streamer over the backup identified by backupID in a
// directory holding backups, laid out the way an object storage would be.
func NewLocal(root, backupID string, opts ...Option) (*Streamer, error) {
	if root == "" {
		return nil, errors.New("root directory must not be empty")
	}

	return newStreamer(&localStore{root: filepath.Clean(root)}, backupID, opts...)
}

// localStore reads a backup out of a directory tree. Paths are the same slash
// separated, root relative paths an object storage uses, and are turned into
// file paths on the way in.
type localStore struct {
	// root is the directory holding the backup directories.
	root string
}

// listLevel walks one directory, without descending into the directories it
// holds.
func (l *localStore) listLevel(ctx context.Context, dir string, fn func(levelEntry) error) error {
	full, err := l.resolve(dir)
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(full)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		// A directory a backup does not have simply holds nothing, which is
		// what an object storage would say about a prefix nothing is under.
		return nil
	case err != nil:
		return fmt.Errorf("read directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		level := levelEntry{Name: entry.Name(), IsDir: entry.IsDir()}

		if !level.IsDir {
			info, err := entry.Info()
			if err != nil {
				return fmt.Errorf("stat %s: %w", path.Join(dir, entry.Name()), err)
			}

			level.file = file{Path: path.Join(dir, entry.Name()), Size: info.Size()}
		}

		if err := fn(level); err != nil {
			return stopped(err)
		}
	}

	return nil
}

// listFiles walks everything below dir, handing over one file at a time.
func (l *localStore) listFiles(ctx context.Context, dir string, fn func(file) error) error {
	full, err := l.resolve(dir)
	if err != nil {
		return err
	}

	err = filepath.WalkDir(full, func(p string, d fs.DirEntry, err error) error {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return filepath.SkipAll
		case err != nil:
			return err
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", p, err)
		}

		if err := fn(file{Path: l.storagePath(p), Size: info.Size()}); err != nil {
			if errors.Is(err, errStopListing) {
				return filepath.SkipAll
			}

			return err
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("walk %s: %w", dir, err)
	}

	return nil
}

// open reads one file of the backup.
func (l *localStore) open(_ context.Context, storagePath string) (io.ReadCloser, error) {
	full, err := l.resolve(storagePath)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(full)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, fmt.Errorf("%w: %s", ErrSegmentMissing, storagePath)
	case err != nil:
		return nil, fmt.Errorf("open %s: %w", storagePath, err)
	}

	return f, nil
}

// resolve turns a storage path into a file path inside the root directory,
// refusing one that would leave it.
func (l *localStore) resolve(storagePath string) (string, error) {
	local := filepath.FromSlash(storagePath)
	if !filepath.IsLocal(local) {
		return "", fmt.Errorf("path %q is outside the root directory", storagePath)
	}

	return filepath.Join(l.root, local), nil
}

// storagePath turns a file path back into the path the rest of the package
// works with.
func (l *localStore) storagePath(p string) string {
	rel, err := filepath.Rel(l.root, p)
	if err != nil {
		return filepath.ToSlash(p)
	}

	return filepath.ToSlash(rel)
}
