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
	"testing"

	"github.com/aerospike/backup-go/encoding"
	"github.com/stretchr/testify/suite"
)

type prepareBackupDirectoryTestSuite struct {
	suite.Suite
}

func (suite *prepareBackupDirectoryTestSuite) TestPrepareBackupDirectory_Positive() {
	dir := suite.T().TempDir()
	err := prepareBackupDirectory(dir)
	suite.NoError(err)
}

func (suite *prepareBackupDirectoryTestSuite) TestPrepareBackupDirectory_Positive_CreateDir() {
	dir := suite.T().TempDir()
	dir = dir + "/test"
	err := prepareBackupDirectory(dir)
	suite.NoError(err)
	suite.DirExists(dir)
}

func (suite *prepareBackupDirectoryTestSuite) TestPrepareBackupDirectory_Negative_IsNotDir() {
	dir := suite.T().TempDir()

	file := dir + "/test"
	f, err := os.Create(file)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}
	f.Close()

	err = prepareBackupDirectory(file)
	suite.Error(err)
}

func (suite *prepareBackupDirectoryTestSuite) TestPrepareBackupDirectory_Negative_DirNotEmpty() {
	dir := suite.T().TempDir()

	file := dir + "/test"
	f, err := os.Create(file)
	if err != nil {
		suite.FailNow("Failed to create file: %v", err)
	}
	f.Close()

	err = prepareBackupDirectory(dir)
	suite.Error(err)
}

func Test_prepareBackupDirectoryTestSuite(t *testing.T) {
	suite.Run(t, new(prepareBackupDirectoryTestSuite))
}

func Test_getBackupFileName(t *testing.T) {
	type args struct {
		namespace string
		id        int
		encoder   EncoderFactory
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "positive ASB",
			args: args{
				namespace: "test",
				id:        1,
				encoder:   encoding.NewASBEncoderFactory(),
			},
			want: "test_1.asb",
		},
		{
			name: "positive no encoder",
			args: args{
				namespace: "test",
				id:        1,
				encoder:   nil,
			},
			want: "test_1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBackupFileName(tt.args.namespace, tt.args.id, tt.args.encoder); got != tt.want {
				t.Errorf("getBackupFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}
