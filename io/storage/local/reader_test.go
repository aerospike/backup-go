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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/internal/util/files"
	"github.com/aerospike/backup-go/io/storage/options"
	optMocks "github.com/aerospike/backup-go/io/storage/options/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCheckRestoreDirectory_Negative_EmptyDir(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestCheckRestoreDirectory_Negative_EmptyDir")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	reader, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithDir(dir),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)
	err = reader.checkRestoreDirectory(dir)
	require.ErrorContains(t, err, "is empty")
}

func TestDirectoryReader_StreamFiles_OK(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamFiles_OK")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	err = createTmpFile(dir, "file1.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file2.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	streamingReader, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithDir(dir),
		options.WithCalculateTotalSize(),
	)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestDirectoryReader_StreamFiles_OneFile(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamFiles_OneFile")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	err = createTmpFile(dir, "file1.asb")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	r, err := NewReader(ctx, options.WithValidator(mockValidator), options.WithDir(dir))
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 1, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestDirectoryReader_StreamFiles_ErrEmptyDir(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamFiles_ErrEmptyDir")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	_, err = NewReader(ctx, options.WithValidator(mockValidator), options.WithDir(dir))
	require.ErrorContains(t, err, "is empty")
}

func TestDirectoryReader_StreamFiles_ErrNoSuchFile(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamFiles_ErrNoSuchFile")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	err = createTmpFile(dir, "file1.asb")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	streamingReader, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithDir("file1.asb"),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.ErrorContains(t, err, "no such file or directory")
			return
		}
	}
}

func TestDirectoryReader_GetType(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_GetType")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	r, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithDir(dir),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	require.Equal(t, localType, r.GetType())
}

func createTmpFile(dir, fileName string) error {
	filePath := filepath.Join(dir, fileName)
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	_, _ = f.WriteString("test content")

	_ = f.Close()

	return nil
}

func createTempNestedDir(rootPath, nestedDir string) error {
	nestedPath := filepath.Join(rootPath, nestedDir)
	if _, err := os.Stat(nestedPath); os.IsNotExist(err) {
		if err = os.MkdirAll(nestedPath, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}
	return nil
}

func TestDirectoryReader_OpenFile(t *testing.T) {
	t.Parallel()
	const fileName = "oneFile.asb"

	dir := path.Join(t.TempDir(), "TestDirectoryReader_OpenFile")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	err = createTmpFile(dir, fileName)
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)

	ctx := t.Context()
	r, err := NewReader(ctx, options.WithValidator(mockValidator), options.WithFile(filepath.Join(dir, fileName)))
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 1, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestDirectoryReader_OpenFileErr(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_OpenFileErr")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	err = createTmpFile(dir, "oneFile.asb")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)

	ctx := t.Context()
	r, err := NewReader(ctx, options.WithValidator(mockValidator), options.WithFile(filepath.Join(dir, "error")))
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 0, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.ErrorContains(t, err, "no such file or directory")
		}
	}
}

func TestDirectoryReader_StreamFiles_Nested_OK(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamFiles_Nested_OK")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	err = createTempNestedDir(dir, "nested1")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(t, err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested2/file2.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	streamingReader, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithDir(dir),
		options.WithNestedDir(),
	)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestDirectoryReader_StreamFilesList(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamFilesList")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	err = createTempNestedDir(dir, "nested1")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(t, err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested2/file2.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		filepath.Join(dir, "nested1", "file1.asb"),
		filepath.Join(dir, "nested2", "file2.asb"),
	}

	ctx := t.Context()

	r, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithFileList(pathList),
	)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestDirectoryReader_StreamPathList(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestDirectoryReader_StreamPathList")

	err := createTempNestedDir(dir, "nested1")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(t, err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested2/file2.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/file3.asb")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		filepath.Join(dir, "nested1"),
		filepath.Join(dir, "nested2"),
	}

	ctx := t.Context()

	r, err := NewReader(
		ctx,
		options.WithValidator(mockValidator),
		options.WithDirList(pathList),
	)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 3, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestReader_WithSorting(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestReader_WithSorting")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	expResult := []string{"0_file_1.asbx", "0_file_2.asbx", "0_file_3.asbx"}

	err = createTmpFile(dir, "0_file_3.asbx")
	require.NoError(t, err)
	err = createTmpFile(dir, "0_file_1.asbx")
	require.NoError(t, err)
	err = createTmpFile(dir, "0_file_2.asbx")
	require.NoError(t, err)
	ctx := t.Context()
	r, err := NewReader(
		ctx,
		options.WithDir(dir),
		options.WithSorting(),
	)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	result := make([]string, 0, 3)
	for {
		select {
		case f, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, expResult, result)
				return
			}
			result = append(result, f.Name)
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestReader_StreamFilesPreloaded(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestReader_StreamFilesPreloaded")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)
	ctx := t.Context()

	expResult := []string{"file3.asb", "0_file_2.asbx", "file1.asb", "file2.asb", "0_file_1.asbx"}

	for i := range expResult {
		err := createTmpFile(dir, expResult[i])
		require.NoError(t, err)
	}

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASBX {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	r, err := NewReader(
		ctx,
		options.WithDir(dir),
		options.WithValidator(mockValidator),
	)
	require.NoError(t, err)

	list, err := r.ListObjects(ctx, dir)
	require.NoError(t, err)
	_, asbxList := filterList(list)
	r.SetObjectsToStream(asbxList)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(t.Context(), readerChan, errorChan, nil)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}
}

func filterList(list []string) (asbList, asbxList []string) {
	for i := range list {
		switch filepath.Ext(list[i]) {
		case files.ExtensionASB:
			asbList = append(asbList, list[i])
		case files.ExtensionASBX:
			asbxList = append(asbxList, list[i])
		}
	}
	return asbList, asbxList
}

func TestReader_ListObjectsWithNestedDir(t *testing.T) {
	dir := path.Join(t.TempDir(), "TestReader_ListObjectsWithNestedDir")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	// Create nested directory structure
	err = createTempNestedDir(dir, "nested1")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(t, err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(t, err)
	err = createTempNestedDir(dir, "nested2/second_level")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested2/second_level/file2.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file3.asb")
	require.NoError(t, err)

	r, err := NewReader(
		t.Context(),
		options.WithDir(dir),
		options.WithNestedDir(),
	)
	require.NoError(t, err)

	list, err := r.ListObjects(t.Context(), dir)
	require.NoError(t, err)
	require.Len(t, list, 3)
	require.Contains(t, list, filepath.Join(dir, "nested1", "file1.asb"))
	require.Contains(t, list, filepath.Join(dir, "nested2", "second_level", "file2.asb"))
	require.Contains(t, list, filepath.Join(dir, "file3.asb"))
}

func TestReader_ListObjectsUnexistingDir(t *testing.T) {
	r, err := NewReader(
		t.Context(),
		options.WithDir("some folder"),
		options.WithNestedDir(),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	listObjects, err := r.ListObjects(t.Context(), "subfolder")
	require.NoError(t, err)
	require.Empty(t, listObjects)
}

func TestReader_StreamFiles_Skipped(t *testing.T) {
	t.Parallel()
	dir := path.Join(t.TempDir(), "TestReader_StreamFiles_Skipped")
	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	const prefix = "meta_"

	err = createTmpFile(dir, prefix+"file1.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file2.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, prefix+"file3.asb")
	require.NoError(t, err)
	err = createTmpFile(dir, "file4.asb")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := t.Context()
	streamingReader, err := NewReader(ctx, options.WithValidator(mockValidator), options.WithDir(dir))
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)

	go streamingReader.StreamFiles(t.Context(), readerChan, errorChan, []string{prefix})

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				require.Equal(t, 2, counter)
				goto Done
			}
			counter++
		case err = <-errorChan:
			require.NoError(t, err)
		}
	}

Done:
	skipped := streamingReader.GetSkipped()
	require.Len(t, skipped, 2)
}

func TestReader_calculateTotalSizeForPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Structure:
	// dir/
	//   file1.asb (12 bytes)
	//   nested1/
	//     file2.asb (12 bytes)
	//     nested2/
	//       file3.asb (12 bytes)
	//   nested3/ (empty, should contribute 0 size 0 count)
	//   other.txt (ignored by validator)

	err := createTmpFile(dir, "file1.asb")
	require.NoError(t, err)

	err = createTempNestedDir(dir, "nested1")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/file2.asb")
	require.NoError(t, err)

	err = createTempNestedDir(dir, "nested1/nested2")
	require.NoError(t, err)
	err = createTmpFile(dir, "nested1/nested2/file3.asb")
	require.NoError(t, err)

	err = createTempNestedDir(dir, "nested3")
	require.NoError(t, err)

	err = createTmpFile(dir, "other.txt")
	require.NoError(t, err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	tests := []struct {
		name      string
		newReader func() (*Reader, error)
		path      string
		wantNum   int64
		wantSize  int64
	}{
		{
			name: "recursive directory",
			newReader: func() (*Reader, error) {
				return NewReader(
					t.Context(),
					options.WithDir(dir),
					options.WithValidator(mockValidator),
					options.WithNestedDir(),
				)
			},
			path:     dir,
			wantNum:  3,
			wantSize: 36,
		},
		{
			name: "non-recursive directory",
			newReader: func() (*Reader, error) {
				return NewReader(
					t.Context(),
					options.WithDir(dir),
					options.WithValidator(mockValidator),
				)
			},
			path:     dir,
			wantNum:  1,
			wantSize: 12,
		},
		{
			name: "single file",
			newReader: func() (*Reader, error) {
				return NewReader(
					t.Context(),
					options.WithFile(filepath.Join(dir, "file1.asb")),
					options.WithValidator(mockValidator),
				)
			},
			path:     filepath.Join(dir, "file1.asb"),
			wantNum:  1,
			wantSize: 12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := tt.newReader()
			require.NoError(t, err)

			size, num, err := r.calculateTotalSizeForPath(tt.path)
			require.NoError(t, err)
			require.Equal(t, tt.wantNum, num)
			require.Equal(t, tt.wantSize, size)
		})
	}
}
