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

package local

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/stretchr/testify/require"
)

func Test_openBackupFile(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_openBackupFile")
	ctx := context.Background()

	factory, err := NewWriter(ctx, ioStorage.WithRemoveFiles(), ioStorage.WithDir(tmpDir))
	require.NoError(t, err)

	w, err := factory.NewWriter(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, w)

	err = w.Close()
	require.NoError(t, err)

	w, err = os.Open(filepath.Join(tmpDir, "test"))
	require.NoError(t, err)
	require.NotNil(t, w)

	err = w.Close()
	require.NoError(t, err)
}

func TestPrepareBackupDirectory_Positive(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestPrepareBackupDirectory_Positive")
	err := createDirIfNotExist(tmpDir, true)
	require.NoError(t, err)
}

func TestPrepareBackupDirectory_Positive_CreateDir(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestPrepareBackupDirectory_Positive_CreateDir")
	err := createDirIfNotExist(tmpDir, true)
	require.NoError(t, err)
	require.DirExists(t, tmpDir)
}

func TestDirectoryWriter_GetType(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_openBackupFile")
	ctx := context.Background()
	w, err := NewWriter(ctx, ioStorage.WithRemoveFiles(), ioStorage.WithDir(tmpDir))
	require.NoError(t, err)

	require.Equal(t, localType, w.GetType())
}
