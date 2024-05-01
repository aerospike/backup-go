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

	"github.com/stretchr/testify/suite"
)

type backupDirectoryTestSuite struct {
	suite.Suite
}

func (suite *backupDirectoryTestSuite) Test_openBackupFile() {
	tmpDir := suite.T().TempDir()

	w, err := openBackupFile(filepath.Join(tmpDir, "test"))
	suite.NoError(err)
	suite.NotNil(w)

	err = w.Close()
	suite.NoError(err)

	w, err = os.Open(filepath.Join(tmpDir, "test"))
	suite.NoError(err)
	suite.NotNil(w)

	err = w.Close()
	suite.NoError(err)
}

func (suite *backupDirectoryTestSuite) Test_getBackupFileNameGeneric() {
	suite.Equal("test_1", getBackupFileNameGeneric("test", 1))
}

func (suite *backupDirectoryTestSuite) Test_getBackupFileNameASB() {
	suite.Equal("test_1.asb", getBackupFileNameASB("test", 1))
}

func (suite *backupDirectoryTestSuite) TestPrepareBackupDirectory_Positive() {
	dir := suite.T().TempDir()
	err := prepareBackupDirectory(dir)
	suite.NoError(err)
}

func (suite *backupDirectoryTestSuite) TestPrepareBackupDirectory_Positive_CreateDir() {
	dir := suite.T().TempDir()
	dir += "/test"
	err := prepareBackupDirectory(dir)
	suite.NoError(err)
	suite.DirExists(dir)
}

func (suite *backupDirectoryTestSuite) TestPrepareBackupDirectory_Negative_IsNotDir() {
	dir := suite.T().TempDir()

	file := dir + "/test"
	f, err := os.Create(file)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}
	_ = f.Close()

	err = prepareBackupDirectory(file)
	suite.Error(err)
}

func (suite *backupDirectoryTestSuite) TestPrepareBackupDirectory_Negative_DirNotEmpty() {
	dir := suite.T().TempDir()

	file := dir + "/test"
	f, err := os.Create(file)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}
	_ = f.Close()

	err = prepareBackupDirectory(dir)
	suite.Error(err)
}

func Test_backupDirectoryTestSuite(t *testing.T) {
	suite.Run(t, new(backupDirectoryTestSuite))
}
