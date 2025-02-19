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

package backup

import (
	"context"
	"path/filepath"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/stretchr/testify/suite"
)

const (
	testSetRestore       = "restore"
	testBackupDirRestore = "restore"
	testDCRestore        = "DC-RESTORE"
	testXDRPortRestore   = 8068
)

type handlerRestoreTestSuite struct {
	suite.Suite
	client *a.Client
}

func TestHandlerRestore(t *testing.T) {
	suite.Run(t, new(handlerRestoreTestSuite))
}

func (s *handlerRestoreTestSuite) SetupTest() {
	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword

	client, aErr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	s.Require().NoError(aErr)

	s.client = client

	err := fillTestData(client, testSetRestore)
	s.Require().NoError(err)
}

func (s *handlerRestoreTestSuite) TearDownSuite() {
}

func (s *handlerRestoreTestSuite) Test_RestoreXDR() {
	bc, err := newBackupClient(s.client)
	s.Require().NoError(err)

	backupDir := filepath.Join(s.T().TempDir(), testBackupDirRestore)
	s.T().Log("backup directory: ", backupDir)

	ctx := context.Background()

	writers, err := local.NewWriter(
		ctx,
		ioStorage.WithValidator(asbx.NewValidator()),
		ioStorage.WithRemoveFiles(),
		ioStorage.WithDir(backupDir),
	)
	s.Require().NoError(err)

	ip := a.NewInfoPolicy()

	backupCfg := &ConfigBackupXDR{
		InfoPolicy:        ip,
		EncryptionPolicy:  nil,
		CompressionPolicy: nil,
		SecretAgentConfig: nil,
		EncoderType:       EncoderTypeASBX,
		FileLimit:         0,
		ParallelWrite:     testParallel,
		DC:                testDCRestore,
		LocalAddress:      testXDRHost,
		LocalPort:         testXDRPortRestore,
		Namespace:         testASNamespace,
		Rewind:            testASRewind,
		TLSConfig:         nil,
		ReadTimeout:       testTimeout,
		WriteTimeout:      testTimeout,
		ResultQueueSize:   testAckQueueSize,
		AckQueueSize:      testResultQueueSize,
		MaxConnections:    testMaxConnections,
		InfoPolingPeriod:  10,
		StartTimeout:      testTimeout,
	}

	backupHandler, err := bc.BackupXDR(ctx, backupCfg, writers)
	s.Require().NoError(err)

	err = backupHandler.Wait(ctx)
	s.Require().NoError(err)

	readers, err := local.NewReader(
		ctx,
		ioStorage.WithDir(backupDir),
		ioStorage.WithValidator(asbx.NewValidator()),
		ioStorage.WithSorting(),
	)
	s.Require().NoError(err)

	restoreCfg := NewDefaultRestoreConfig()
	restoreCfg.WritePolicy = s.client.GetDefaultWritePolicy()
	restoreCfg.EncoderType = EncoderTypeASBX
	namespace := testASNamespace
	restoreCfg.Namespace = &RestoreNamespaceConfig{
		Source:      &namespace,
		Destination: &namespace,
	}

	restoreHandler, err := bc.Restore(ctx, restoreCfg, readers)
	s.Require().NoError(err)

	err = restoreHandler.Wait(ctx)
	s.Require().NoError(err)
}
