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
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/stretchr/testify/suite"
)

const (
	testASLoginPassword = "admin"
	testASNamespace     = "test"
	testSetBackupXDR    = "xdr"
	testDCXDR           = "DC-XDR"
	testDCFileLimit     = "DC-XDR-File-Limit"
	testASHost          = "127.0.0.1"
	testASPort          = 3000
	testASRewind        = "all"

	testBackupDir           = "xdr_backup"
	testBackupDirLimit      = "xdr_backup_limit"
	testParallel            = 8
	testXDRHost             = "172.17.0.1"
	testXDRPort             = 8066
	testXDRPortFileLimit    = 8067
	testTimeoutMilliseconds = 10000
	testAckQueueSize        = 256
	testResultQueueSize     = 256
	testMaxConnections      = 100
)

type handlerBackupXDRTestSuite struct {
	suite.Suite
	client *a.Client
}

func TestHandlerBackupXDR(t *testing.T) {
	suite.Run(t, new(handlerBackupXDRTestSuite))
}

func (s *handlerBackupXDRTestSuite) SetupTest() {
	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword

	client, aErr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	s.Require().NoError(aErr)

	s.client = client

	err := fillTestData(client, testSetBackupXDR)
	s.Require().NoError(err)
}

func fillTestData(client *a.Client, setName string) error {
	wp := a.NewWritePolicy(0, 0)

	var (
		key  *a.Key
		bin1 *a.Bin
	)

	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key, _ = a.NewKey(testASNamespace, setName, fmt.Sprintf("map-key-%d", i))
			bin1 = generateMap()
		} else {
			key, _ = a.NewKey(testASNamespace, setName, fmt.Sprintf("list-key-%d", i))
			bin1 = generateList()
		}

		if err := client.PutBins(wp, key, bin1); err != nil {
			return err
		}
	}

	return nil
}

func generateMap() *a.Bin {
	mapBin := a.NewBin("myMapBin", map[interface{}]interface{}{
		"name":    generateRandomString(10),
		"age":     rand.Int(),
		"balance": rand.Float64(),
	})
	return mapBin
}

func generateList() *a.Bin {
	listBin := a.NewBin(
		"myListBin", []interface{}{
			rand.Int(),
			rand.Int(),
			generateRandomString(10),
			rand.Float64(),
			true,
		})
	return listBin
}

func generateRandomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]rune, n)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)
}

func (s *handlerBackupXDRTestSuite) TearDownSuite() {
}

func (s *handlerBackupXDRTestSuite) Test_Backup() {
	bc, err := newBackupClient(s.client)
	s.Require().NoError(err)

	backupDir := filepath.Join(s.T().TempDir(), testBackupDir)
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
		InfoPolicy:                   ip,
		EncryptionPolicy:             nil,
		CompressionPolicy:            nil,
		SecretAgentConfig:            nil,
		EncoderType:                  EncoderTypeASBX,
		FileLimit:                    0,
		ParallelWrite:                testParallel,
		DC:                           testDCXDR,
		LocalAddress:                 testXDRHost,
		LocalPort:                    testXDRPort,
		Namespace:                    testASNamespace,
		Rewind:                       testASRewind,
		TLSConfig:                    nil,
		ReadTimeoutMilliseconds:      testTimeoutMilliseconds,
		WriteTimeoutMilliseconds:     testTimeoutMilliseconds,
		ResultQueueSize:              testAckQueueSize,
		AckQueueSize:                 testResultQueueSize,
		MaxConnections:               testMaxConnections,
		InfoPolingPeriodMilliseconds: 10,
		StartTimeoutMilliseconds:     testTimeoutMilliseconds,
	}

	backupHandler, err := bc.BackupXDR(ctx, backupCfg, writers)
	s.Require().NoError(err)

	err = backupHandler.Wait(ctx)
	s.Require().NoError(err)

	fileInfo, err := os.ReadDir(backupDir)
	s.Require().NoError(err)

	s.Require().Len(fileInfo, testParallel)
}

func (s *handlerBackupXDRTestSuite) Test_BackupFileLimit() {
	bc, err := newBackupClient(s.client)
	s.Require().NoError(err)

	backupDir := filepath.Join(s.T().TempDir(), testBackupDirLimit)
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
		InfoPolicy:                   ip,
		EncryptionPolicy:             nil,
		CompressionPolicy:            nil,
		SecretAgentConfig:            nil,
		EncoderType:                  EncoderTypeASBX,
		FileLimit:                    1000,
		ParallelWrite:                testParallel,
		DC:                           testDCFileLimit,
		LocalAddress:                 testXDRHost,
		LocalPort:                    testXDRPortFileLimit,
		Namespace:                    testASNamespace,
		Rewind:                       testASRewind,
		TLSConfig:                    nil,
		ReadTimeoutMilliseconds:      testTimeoutMilliseconds,
		WriteTimeoutMilliseconds:     testTimeoutMilliseconds,
		ResultQueueSize:              testAckQueueSize,
		AckQueueSize:                 testResultQueueSize,
		MaxConnections:               testMaxConnections,
		InfoPolingPeriodMilliseconds: 10,
		StartTimeoutMilliseconds:     testTimeoutMilliseconds,
	}

	backupHandler, err := bc.BackupXDR(ctx, backupCfg, writers)
	s.Require().NoError(err)

	err = backupHandler.Wait(ctx)
	s.Require().NoError(err)

	fileInfo, err := os.ReadDir(backupDir)
	s.Require().NoError(err)

	s.Require().Greater(len(fileInfo), testParallel)
}

func newBackupClient(aerospikeClient *a.Client) (*Client, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		// Uncomment for test debugging.
		// Level: slog.LevelDebug,
	}))

	backupClient, err := NewClient(
		aerospikeClient,
		WithID("xdr_test_id"),
		WithLogger(logger),
	)
	if err != nil {
		return nil, err
	}

	return backupClient, nil
}
