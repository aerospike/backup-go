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
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/internal/util/files"
	"github.com/aerospike/backup-go/io/storage/options"
	optMocks "github.com/aerospike/backup-go/io/storage/options/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testFileName = "test.asb"

func Test_openBackupFile(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_openBackupFile")
	ctx := t.Context()

	factory, err := NewWriter(ctx, options.WithRemoveFiles(), options.WithDir(tmpDir))
	require.NoError(t, err)

	w, err := factory.NewWriter(t.Context(), "test")
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
	ctx := t.Context()
	w, err := NewWriter(ctx, options.WithRemoveFiles(), options.WithDir(tmpDir))
	require.NoError(t, err)

	require.Equal(t, localType, w.GetType())
}

func TestNewWriter_NoPath(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, err := NewWriter(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "one path is required")
}

func TestNewWriter_WithFile(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestNewWriter_WithFile")
	filePath := filepath.Join(tmpDir, "test.asb")
	ctx := t.Context()

	w, err := NewWriter(ctx, options.WithFile(filePath))
	require.NoError(t, err)
	require.NotNil(t, w)

	require.Equal(t, filePath, w.PathList[0])
	require.False(t, w.IsDir)
}

func TestNewWriter_WithDir_NonExistent(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestNewWriter_WithDir_NonExistent", "nonexistent")
	ctx := t.Context()

	w, err := NewWriter(ctx, options.WithDir(tmpDir))
	require.NoError(t, err)
	require.NotNil(t, w)

	require.Equal(t, tmpDir, w.PathList[0])
	require.True(t, w.IsDir)
}

func TestNewWriter_WithDir_Empty(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestNewWriter_WithDir_Empty")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	ctx := t.Context()
	w, err := NewWriter(ctx, options.WithDir(tmpDir))
	require.NoError(t, err)
	require.NotNil(t, w)
}

func TestNewWriter_WithDir_NonEmpty_NoRemove(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestNewWriter_WithDir_NonEmpty_NoRemove")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	ctx := t.Context()
	_, err = NewWriter(ctx, options.WithDir(tmpDir))
	require.Error(t, err)
	require.Contains(t, err.Error(), "backup folder must be empty or set RemoveFiles = true")
}

func TestNewWriter_WithDir_NonEmpty_WithRemove(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestNewWriter_WithDir_NonEmpty_WithRemove")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	ctx := t.Context()
	w, err := NewWriter(ctx, options.WithDir(tmpDir), options.WithRemoveFiles())
	require.NoError(t, err)
	require.NotNil(t, w)

	// Verify the file was removed
	_, err = os.Stat(filePath)
	require.True(t, os.IsNotExist(err))
}

func TestNewWriter_WithDir_SkipDirCheck(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestNewWriter_WithDir_SkipDirCheck")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	ctx := t.Context()
	w, err := NewWriter(ctx, options.WithDir(tmpDir), options.WithSkipDirCheck())
	require.NoError(t, err)
	require.NotNil(t, w)

	_, err = os.Stat(filePath)
	require.NoError(t, err)
}

func Test_createDirIfNotExist_DirExists(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_createDirIfNotExist_DirExists")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	err = createDirIfNotExist(tmpDir, true)
	require.NoError(t, err)
}

func Test_createDirIfNotExist_DirNotExists(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_createDirIfNotExist_DirNotExists", "nonexistent")

	err := createDirIfNotExist(tmpDir, true)
	require.NoError(t, err)

	_, err = os.Stat(tmpDir)
	require.NoError(t, err)
}

func Test_createDirIfNotExist_FilePath(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_createDirIfNotExist_FilePath")
	filePath := filepath.Join(tmpDir, testFileName)

	err := createDirIfNotExist(filePath, false)
	require.NoError(t, err)

	// Verify the directory was created
	_, err = os.Stat(tmpDir)
	require.NoError(t, err)
}

func Test_isEmptyDirectory_Empty(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_isEmptyDirectory_Empty")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	isEmpty, err := isEmptyDirectory(tmpDir)
	require.NoError(t, err)
	require.True(t, isEmpty)
}

func Test_isEmptyDirectory_NonEmpty(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_isEmptyDirectory_NonEmpty")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	isEmpty, err := isEmptyDirectory(tmpDir)
	require.NoError(t, err)
	require.False(t, isEmpty)
}

func Test_isEmptyDirectory_NonExistent(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "Test_isEmptyDirectory_NonExistent", "nonexistent")

	_, err := isEmptyDirectory(tmpDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read path")
}

func TestWriter_RemoveFiles_NonExistent(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_RemoveFiles_NonExistent", "nonexistent")
	ctx := t.Context()

	w := &Writer{
		Options: options.Options{
			PathList: []string{tmpDir},
			IsDir:    true,
		},
	}

	err := w.RemoveFiles(ctx)
	require.NoError(t, err)
}

func TestWriter_RemoveFiles_File(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_RemoveFiles_File")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	ctx := t.Context()
	w := &Writer{
		Options: options.Options{
			PathList: []string{filePath},
			IsDir:    false,
		},
	}

	err = w.RemoveFiles(ctx)
	require.NoError(t, err)

	_, err = os.Stat(filePath)
	require.True(t, os.IsNotExist(err))
}

func TestWriter_RemoveFiles_Dir_WithNestedDir(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_RemoveFiles_Dir_WithNestedDir")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	nestedDir := filepath.Join(tmpDir, "nested")
	err = os.MkdirAll(nestedDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(nestedDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	ctx := t.Context()
	w := &Writer{
		Options: options.Options{
			PathList:      []string{tmpDir},
			IsDir:         true,
			WithNestedDir: true,
		},
	}

	err = w.RemoveFiles(ctx)
	require.NoError(t, err)

	_, err = os.Stat(tmpDir)
	require.True(t, os.IsNotExist(err))
}

func TestWriter_RemoveFiles_Dir_WithValidator(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_RemoveFiles_Dir_WithValidator")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	validFilePath := filepath.Join(tmpDir, "valid.asb")
	validFile, err := os.Create(validFilePath)
	require.NoError(t, err)
	err = validFile.Close()
	require.NoError(t, err)

	invalidFilePath := filepath.Join(tmpDir, "invalid.txt")
	invalidFile, err := os.Create(invalidFilePath)
	require.NoError(t, err)
	err = invalidFile.Close()
	require.NoError(t, err)

	nestedDir := filepath.Join(tmpDir, "nested")
	err = os.MkdirAll(nestedDir, os.ModePerm)
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	ctx := t.Context()
	w := &Writer{
		Options: options.Options{
			PathList:  []string{tmpDir},
			IsDir:     true,
			Validator: mockValidator,
		},
	}

	err = w.RemoveFiles(ctx)
	require.NoError(t, err)

	_, err = os.Stat(validFilePath)
	require.True(t, os.IsNotExist(err))

	_, err = os.Stat(invalidFilePath)
	require.NoError(t, err)
	_, err = os.Stat(nestedDir)
	require.NoError(t, err)
}

func TestWriter_RemoveFiles_CanceledContext(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_RemoveFiles_CanceledContext")

	ctx, cancel := context.WithCancel(t.Context())
	// Cancel the context immediately
	cancel()

	w := &Writer{
		Options: options.Options{
			PathList: []string{tmpDir},
			IsDir:    true,
		},
	}

	err := w.RemoveFiles(ctx)
	require.Error(t, err)
	require.Equal(t, ctx.Err(), err)
}

func TestWriter_NewWriter_CanceledContext(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_NewWriter_CanceledContext")

	ctx, cancel := context.WithCancel(t.Context())
	// Cancel the context immediately
	cancel()

	w := &Writer{
		Options: options.Options{
			PathList: []string{tmpDir},
			IsDir:    true,
		},
	}

	_, err := w.NewWriter(ctx, testFileName)
	require.Error(t, err)
	require.Equal(t, ctx.Err(), err)
}

func TestWriter_NewWriter_CreateDirError(t *testing.T) {
	t.Parallel()
	// Use a path that cannot be created (e.g., a path with invalid characters)
	tmpDir := "\000invalid"

	w := &Writer{
		Options: options.Options{
			PathList: []string{tmpDir},
			IsDir:    true,
		},
	}

	_, err := w.NewWriter(t.Context(), testFileName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to prepare backup directory")
}

func TestWriter_NewWriter_WithFile(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_NewWriter_WithFile")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)

	w := &Writer{
		Options: options.Options{
			PathList: []string{filePath},
			IsDir:    false,
		},
	}

	writer, err := w.NewWriter(t.Context(), "")
	require.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, "test data", string(content))
}

func TestWriter_NewWriter_WithDir(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestWriter_NewWriter_WithDir")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	w := &Writer{
		Options: options.Options{
			PathList: []string{tmpDir},
			IsDir:    true,
		},
	}

	fileName := testFileName
	writer, err := w.NewWriter(t.Context(), fileName)
	require.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, fileName)
	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, "test data", string(content))
}

func TestWriter_GetOptions(t *testing.T) {
	t.Parallel()

	o1 := options.Options{
		PathList: []string{testFileName},
		IsDir:    false,
	}

	w := &Writer{
		o1,
	}

	o2 := w.GetOptions()
	require.Equal(t, o1, o2)
}

func TestBufferedFile_Close_Error(t *testing.T) {
	t.Parallel()
	tmpDir := path.Join(t.TempDir(), "TestBufferedFile_Close_Error")
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err)

	filePath := filepath.Join(tmpDir, testFileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	mockCloser := &mockCloser{
		file: file,
		err:  fmt.Errorf("mock close error"),
	}

	bf := &bufferedFile{
		Writer: bufio.NewWriter(mockCloser),
		closer: mockCloser,
	}

	_, err = bf.WriteString("test data")
	require.NoError(t, err)

	err = bf.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock close error")
}

// mockCloser implements io.WriteCloser and returns a specified error on Close
type mockCloser struct {
	file *os.File
	err  error
}

func (m *mockCloser) Write(p []byte) (n int, err error) {
	return m.file.Write(p)
}

func (m *mockCloser) Close() error {
	_ = m.file.Close()
	return m.err
}
