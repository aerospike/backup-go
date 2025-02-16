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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/local/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type readerTestSuite struct {
	suite.Suite
}

func (s *readerTestSuite) TestCheckRestoreDirectory_Negative_EmptyDir() {
	dir := s.T().TempDir()
	err := createTmpFile(dir, "file3.txt")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	reader, err := NewReader(
		ctx,
		ioStorage.WithValidator(mockValidator),
		ioStorage.WithDir(dir),
		ioStorage.WithSkipDirCheck(),
	)
	s.NoError(err)
	err = reader.checkRestoreDirectory(dir)
	s.Error(err)
}

func TestCheckRestoreDirectory(t *testing.T) {
	suite.Run(t, new(readerTestSuite))
}

func (s *readerTestSuite) TestDirectoryReader_StreamFiles_OK() {
	dir := s.T().TempDir()

	err := createTmpFile(dir, "file1.asb")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "file2.asb")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	streamingReader, err := NewReader(ctx, ioStorage.WithValidator(mockValidator), ioStorage.WithDir(dir))
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_StreamFiles_OneFile() {
	dir := s.T().TempDir()
	err := createTmpFile(dir, "file1.asb")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	r, err := NewReader(ctx, ioStorage.WithValidator(mockValidator), ioStorage.WithDir(dir))
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(1, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_StreamFiles_ErrEmptyDir() {
	dir := s.T().TempDir()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	_, err := NewReader(ctx, ioStorage.WithValidator(mockValidator), ioStorage.WithDir(dir))
	s.Require().ErrorContains(err, "is empty")
}

func (s *readerTestSuite) TestDirectoryReader_StreamFiles_ErrNoSuchFile() {
	dir := s.T().TempDir()
	err := createTmpFile(dir, "file1.asb")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	streamingReader, err := NewReader(
		ctx,
		ioStorage.WithValidator(mockValidator),
		ioStorage.WithDir("file1.asb"),
		ioStorage.WithSkipDirCheck(),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.ErrorContains(s.T(), err, "no such file or directory")
			return
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_GetType() {
	dir := s.T().TempDir()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	r, err := NewReader(
		ctx,
		ioStorage.WithValidator(mockValidator),
		ioStorage.WithDir(dir),
		ioStorage.WithSkipDirCheck(),
	)
	s.Require().NoError(err)

	s.Equal(localType, r.GetType())
}

func createTmpFile(dir, fileName string) error {
	filePath := filepath.Join(dir, fileName)
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
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

func (s *readerTestSuite) TestDirectoryReader_OpenFile() {
	const fileName = "oneFile.asb"

	dir := s.T().TempDir()
	err := createTmpFile(dir, fileName)
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)

	path := filepath.Join(dir, fileName)
	ctx := context.Background()
	r, err := NewReader(ctx, ioStorage.WithValidator(mockValidator), ioStorage.WithFile(path))
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(1, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_OpenFileErr() {
	dir := s.T().TempDir()
	err := createTmpFile(dir, "oneFile.asb")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)

	path := filepath.Join(dir, "error")
	ctx := context.Background()
	r, err := NewReader(ctx, ioStorage.WithValidator(mockValidator), ioStorage.WithFile(path))
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(0, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.ErrorContains(s.T(), err, "no such file or directory")
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_StreamFiles_Nested_OK() {
	dir := s.T().TempDir()

	err := createTempNestedDir(dir, "nested1")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(s.T(), err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested2/file2.asb")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})
	ctx := context.Background()
	streamingReader, err := NewReader(
		ctx,
		ioStorage.WithValidator(mockValidator),
		ioStorage.WithDir(dir),
		ioStorage.WithNestedDir(),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_StreamFilesList() {
	dir := s.T().TempDir()

	err := createTempNestedDir(dir, "nested1")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(s.T(), err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested2/file2.asb")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "file3.txt")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		filepath.Join(dir, "nested1", "file1.asb"),
		filepath.Join(dir, "nested2", "file2.asb"),
	}

	ctx := context.Background()

	r, err := NewReader(
		ctx,
		ioStorage.WithValidator(mockValidator),
		ioStorage.WithFileList(pathList),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestDirectoryReader_StreamPathList() {
	dir := s.T().TempDir()

	err := createTempNestedDir(dir, "nested1")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested1/file1.asb")
	require.NoError(s.T(), err)
	err = createTempNestedDir(dir, "nested2")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested2/file2.asb")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "nested1/file3.asb")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		filepath.Join(dir, "nested1"),
		filepath.Join(dir, "nested2"),
	}

	ctx := context.Background()

	r, err := NewReader(
		ctx,
		ioStorage.WithValidator(mockValidator),
		ioStorage.WithDirList(pathList),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(3, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestReader_WithSorting() {
	dir := s.T().TempDir()

	expResult := []string{"0_file_1.asbx", "0_file_2.asbx", "0_file_3.asbx"}

	err := createTmpFile(dir, "0_file_3.asbx")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "0_file_1.asbx")
	require.NoError(s.T(), err)
	err = createTmpFile(dir, "0_file_2.asbx")
	require.NoError(s.T(), err)
	ctx := context.Background()
	r, err := NewReader(
		ctx,
		ioStorage.WithDir(dir),
		ioStorage.WithSorting(),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	result := make([]string, 0, 3)
	for {
		select {
		case f, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Equal(expResult, result)
				return
			}
			result = append(result, f.Name)
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func (s *readerTestSuite) TestReader_StreamFilesPreloaded() {
	dir := s.T().TempDir()
	ctx := context.Background()

	expResult := []string{"file3.asb", "file2.asbx", "file1.asb", "file2.asb", "file1.asbx"}

	for i := range expResult {
		err := createTmpFile(dir, expResult[i])
		require.NoError(s.T(), err)
	}

	r, err := NewReader(
		ctx,
		ioStorage.WithDir(dir),
	)
	s.Require().NoError(err)

	list, err := r.ListObjects(ctx, dir)
	s.Require().NoError(err)
	_, asbxList := filterList(list)
	r.SetObjectsToStream(asbxList)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go r.StreamFiles(context.Background(), readerChan, errorChan)

	var counter int
	for {
		select {
		case _, ok := <-readerChan:
			// if chan closed, we're done.
			if !ok {
				s.Require().Equal(2, counter)
				return
			}
			counter++
		case err = <-errorChan:
			require.NoError(s.T(), err)
		}
	}
}

func filterList(list []string) (asbList, asbxList []string) {
	for i := range list {
		switch filepath.Ext(list[i]) {
		case ".asb":
			asbList = append(asbList, list[i])
		case ".asbx":
			asbxList = append(asbxList, list[i])
		}
	}
	return asbList, asbxList
}
