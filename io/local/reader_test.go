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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/io/local/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type checkRestoreDirectoryTestSuite struct {
	suite.Suite
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_EmptyDir() {
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

	reader, err := NewReader(WithValidator(mockValidator), WithDir(dir))
	s.NoError(err)
	err = reader.checkRestoreDirectory(dir)
	s.Error(err)
}

func TestCheckRestoreDirectory(t *testing.T) {
	suite.Run(t, new(checkRestoreDirectoryTestSuite))
}

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFiles_OK() {
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

	streamingReader, err := NewReader(WithValidator(mockValidator), WithDir(dir))
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFiles_OneFile() {
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

	r, err := NewReader(WithValidator(mockValidator), WithDir(dir))
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFiles_ErrEmptyDir() {
	dir := s.T().TempDir()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	streamingReader, err := NewReader(WithValidator(mockValidator), WithDir(dir))
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
	errorChan := make(chan error)
	go streamingReader.StreamFiles(context.Background(), readerChan, errorChan)

	for err = range errorChan {
		s.Require().ErrorContains(err, "is empty")
		return
	}
}

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFiles_ErrNoSuchFile() {
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

	streamingReader, err := NewReader(WithValidator(mockValidator), WithDir("file1.asb"))
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_GetType() {
	dir := s.T().TempDir()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	r, err := NewReader(WithValidator(mockValidator), WithDir(dir))
	s.Require().NoError(err)

	s.Equal(localType, r.GetType())
}

func createTmpFile(dir, fileName string) error {
	filePath := filepath.Join(dir, fileName)
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_OpenFile() {
	const fileName = "oneFile.asb"

	dir := s.T().TempDir()
	err := createTmpFile(dir, fileName)
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)

	path := filepath.Join(dir, fileName)
	r, err := NewReader(WithValidator(mockValidator), WithFile(path))
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_OpenFileErr() {
	dir := s.T().TempDir()
	err := createTmpFile(dir, "oneFile.asb")
	require.NoError(s.T(), err)

	mockValidator := new(mocks.Mockvalidator)

	path := filepath.Join(dir, "error")
	r, err := NewReader(WithValidator(mockValidator), WithFile(path))
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFiles_Nested_OK() {
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

	streamingReader, err := NewReader(WithValidator(mockValidator), WithDir(dir), WithNestedDir())
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFilesList() {
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

	r, err := NewReader(
		WithValidator(mockValidator),
		WithFileList(pathList),
	)
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamPathList() {
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

	r, err := NewReader(
		WithValidator(mockValidator),
		WithDirList(pathList),
	)
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
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
