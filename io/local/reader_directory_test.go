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

	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/stretchr/testify/suite"
)

type checkRestoreDirectoryTestSuite struct {
	suite.Suite
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_SingleFile() {
	dir := s.T().TempDir()
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	f.Close()

	factory, _ := NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	s.NoError(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_nilDecoder() {
	dir := s.T().TempDir()
	_, err := NewDirectoryReaderFactory(dir, nil)
	s.Error(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_MultipleFiles() {
	dir := s.T().TempDir()
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	f.Close()

	file = "file2.asb"
	filePath = filepath.Join(dir, file)

	f, err = os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	f.Close()

	factory, err := NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	s.NoError(err)
	_, err = factory.Readers()
	s.NoError(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_BadExtension() {
	dir := s.T().TempDir()
	file := "file1"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	f.Close()

	factory, _ := NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	s.Error(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_NotADir() {
	dir := s.T().TempDir()
	file, err := os.CreateTemp(dir, "")
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	file.Close()

	path := filepath.Join(dir, file.Name())

	factory, _ := NewDirectoryReaderFactory(path, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	s.Error(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_ContainsDir() {
	dir := s.T().TempDir()
	file := "file1"
	filePath := filepath.Join(dir, file)

	err := os.Mkdir(filePath, 0o755)
	if err != nil {
		s.FailNow("Failed to create dir: %v", err)
	}

	factory, _ := NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	s.Error(err)
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_EmptyDir() {
	dir := s.T().TempDir()
	factory, _ := NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	err := factory.checkRestoreDirectory()
	s.Error(err)
}

func TestCheckRestoreDirectory(t *testing.T) {
	suite.Run(t, new(checkRestoreDirectoryTestSuite))
}

func (s *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_ReadToChan() {
	dir := s.T().TempDir()
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	f.Close()

	file = "file2.asb"
	filePath = filepath.Join(dir, file)

	f, err = os.Create(filePath)
	if err != nil {
		s.FailNow("Failed to create file: %v", err)
	}

	f.Close()

	fac, err := NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
	errorChan := make(chan error)
	go fac.ReadToChan(context.Background(), readerChan, errorChan)

	var counter int
	for msg := range readerChan {
		fmt.Println(msg)
		counter++
	}

	s.Require().Equal(2, counter)
}
