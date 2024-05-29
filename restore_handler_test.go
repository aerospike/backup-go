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

package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/io/local"
	"github.com/stretchr/testify/suite"
)

type checkRestoreDirectoryTestSuite struct {
	suite.Suite
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_SingleFile() {
	dir := suite.T().TempDir()
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	factory, _ := local.NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	suite.NoError(err)
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_nilDecoder() {
	dir := suite.T().TempDir()
	_, err := local.NewDirectoryReaderFactory(dir, nil)
	suite.Error(err)
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Positive_MultipleFiles() {
	dir := suite.T().TempDir()
	file := "file1.asb"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	file = "file2.asb"
	filePath = filepath.Join(dir, file)

	f, err = os.Create(filePath)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	factory, err := local.NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	suite.NoError(err)
	_, err = factory.Readers()
	suite.NoError(err)
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_BadExtension() {
	dir := suite.T().TempDir()
	file := "file1"
	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}

	_ = f.Close()

	factory, _ := local.NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	suite.Error(err)
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_NotADir() {
	dir := suite.T().TempDir()
	file, err := os.CreateTemp(dir, "")
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}

	_ = file.Close()

	path := filepath.Join(dir, file.Name())

	factory, _ := local.NewDirectoryReaderFactory(path, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	suite.Error(err)
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_ContainsDir() {
	dir := suite.T().TempDir()
	file := "file1"
	filePath := filepath.Join(dir, file)

	err := os.Mkdir(filePath, 0o755)
	if err != nil {
		suite.FailNow("Failed to create dir: %v", err)
	}

	factory, _ := local.NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	_, err = factory.Readers()
	suite.Error(err)
}

func (suite *checkRestoreDirectoryTestSuite) TestCheckRestoreDirectory_Negative_EmptyDir() {
	dir := suite.T().TempDir()
	factory, _ := local.NewDirectoryReaderFactory(dir, asb.NewASBDecoderFactory())
	err := factory.checkRestoreDirectory()
	suite.Error(err)
}

func TestCheckRestoreDirectory(t *testing.T) {
	suite.Run(t, new(checkRestoreDirectoryTestSuite))
}
