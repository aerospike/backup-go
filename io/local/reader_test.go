// Copyright 2024-2024 Aerospike, Inc.
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

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_nilDecoder() {
	dir := s.T().TempDir()
	_, err := NewDirectoryStreamingReader(dir, nil)
	s.Error(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_EmptyDir() {
	dir := s.T().TempDir()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	streamingReader, _ := NewDirectoryStreamingReader(dir, mockValidator)
	err := streamingReader.checkRestoreDirectory()
	s.Error(err)
}

func TestCheckRestoreDirectory(t *testing.T) {
	suite.Run(t, new(checkRestoreDirectoryTestSuite))
}

func (s *checkRestoreDirectoryTestSuite) TestDirectoryReader_StreamFiles_OK() {
	dir := s.T().TempDir()
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	file = "file2.asb"
	filePath = filepath.Join(dir, file)

	f, err = os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	streamingReader, err := NewDirectoryStreamingReader(dir, mockValidator)
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
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	r, err := NewDirectoryStreamingReader(dir, mockValidator)
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