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

package backup_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	testresources "github.com/aerospike/backup-go/internal/testutils"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/testutils"
	"github.com/stretchr/testify/suite"
)

const (
	// got this from writing and reading back a.HLLAddOp(hllpol, "hll", []a.Value{a.NewIntegerValue(1)}, 4, 12)
	//nolint:lll // can't split this up without making it a raw quote which will cause the escaped bytes to be interpreted literally
	hllValue = "\x00\x04\f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x84\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)

// testBins is a collection of all supported bin types
// useful for testing backup and restore
var testBins = a.BinMap{
	"IntBin":     1,
	"FloatBin":   1.1,
	"StringBin":  "string",
	"BoolBin":    true,
	"BlobBin":    []byte("bytes"),
	"GeoJSONBin": a.GeoJSONValue(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
	"HLLBin":     a.NewHLLValue([]byte(hllValue)),
	"MapBin": map[any]any{
		"IntBin":    1,
		"StringBin": "hi",
		"listBin":   []any{1, 2, 3},
		"mapBin":    map[any]any{1: 1},
	},
	"ListBin": []any{
		1,
		"string",
		[]byte("bytes"),
		map[any]any{1: 1},
		[]any{1, 2, 3},
	},
}

type backupRestoreTestSuite struct {
	suite.Suite
	aerospikeIP       string
	aerospikePort     int
	aerospikePassword string
	aerospikeUser     string
	namespace         string
	set               string
	Aeroclient        *a.Client
	testClient        *testresources.TestClient
	backupClient      *backup.Client
}

func (suite *backupRestoreTestSuite) SetupSuite() {
	testutils.Image = "aerospike/aerospike-server-enterprise:7.0.0.2"

	clusterSize := 1
	err := testutils.Start(clusterSize)
	if err != nil {
		suite.FailNow(err.Error())
	}

	aeroClientPolicy := a.NewClientPolicy()
	aeroClientPolicy.User = suite.aerospikeUser
	aeroClientPolicy.Password = suite.aerospikePassword

	asc, aerr := a.NewClientWithPolicy(
		aeroClientPolicy,
		suite.aerospikeIP,
		suite.aerospikePort,
	)
	if aerr != nil {
		suite.FailNow(aerr.Error())
		return
	}
	defer asc.Close()

	privs := []a.Privilege{
		{Code: a.Read},
		{Code: a.Write},
		{Code: a.Truncate},
		{Code: a.UserAdmin},
	}

	aerr = asc.CreateRole(nil, "testBackup", privs, nil, 0, 0)
	if aerr != nil {
		suite.FailNow(aerr.Error())
	}

	aerr = asc.CreateUser(nil, "backupTester", "changeme", []string{"testBackup"})
	if aerr != nil {
		suite.FailNow(aerr.Error())
	}

	asc.Close()

	aeroClientPolicy.User = "backupTester"
	aeroClientPolicy.Password = "changeme"
	testAeroClient, aerr := a.NewClientWithPolicy(
		aeroClientPolicy,
		suite.aerospikeIP,
		suite.aerospikePort,
	)
	if aerr != nil {
		suite.FailNow(aerr.Error())
	}

	suite.Aeroclient = testAeroClient

	testClient := testresources.NewTestClient(testAeroClient)
	suite.testClient = testClient

	backupCFG := backup.Config{}
	backupClient, err := backup.NewClient(testAeroClient, "test_client", slog.Default(), &backupCFG)
	if err != nil {
		suite.FailNow(err.Error())
	}

	suite.backupClient = backupClient
}

func (suite *backupRestoreTestSuite) TearDownSuite() {
	defer func() {
		err := testutils.Stop()
		if err != nil {
			suite.FailNow(err.Error())
		}
	}()

	asc := suite.Aeroclient
	defer asc.Close()

	aerr := asc.DropRole(nil, "testBackup")
	if aerr != nil {
		suite.FailNow(aerr.Error())
	}

	aerr = asc.DropUser(nil, "backupTester")
	if aerr != nil {
		suite.FailNow(aerr.Error())
	}
}

func (suite *backupRestoreTestSuite) SetupTest() {
	err := suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		suite.FailNow(err.Error())
	}
}

func (suite *backupRestoreTestSuite) TearDownTest() {
	err := suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		suite.FailNow(err.Error())
	}
}

func (suite *backupRestoreTestSuite) TestBackupRestoreIO() {
	type args struct {
		backupConfig  *backup.BackupConfig
		restoreConfig *backup.RestoreConfig
		bins          a.BinMap
	}
	var tests = []struct {
		name string
		args args
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewBackupConfig(),
				restoreConfig: backup.NewRestoreConfig(),
				bins:          testBins,
			},
		},
	}
	for _, tt := range tests {
		suite.SetupTest()
		suite.Run(tt.name, func() {
			runBackupRestore(suite, tt.args.backupConfig, tt.args.restoreConfig, tt.args.bins)
		})
		suite.TearDownTest()
	}
}

func runBackupRestore(suite *backupRestoreTestSuite, backupConfig *backup.BackupConfig,
	restoreConfig *backup.RestoreConfig, bins a.BinMap) {
	numRec := 1
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	dst := ByteReaderWriterFactory{buffer: bytes.NewBuffer([]byte{})}

	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		&dst,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	err = bh.Wait(ctx)
	suite.Nil(err)

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		panic(err)
	}

	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		&dst,
	)
	suite.Nil(err)
	suite.NotNil(rh)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.testClient.ValidateRecords(suite.T(), expectedRecs, numRec, suite.namespace, suite.set)
}

func (suite *backupRestoreTestSuite) TestBackupRestoreDirectory() {
	type args struct {
		backupConfig  *backup.BackupConfig
		restoreConfig *backup.RestoreConfig
		bins          a.BinMap
	}
	var tests = []struct {
		name string
		args args
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewBackupConfig(),
				restoreConfig: backup.NewRestoreConfig(),
				bins:          testBins,
			},
		},
		{
			name: "with parallel backup",
			args: args{
				backupConfig: &backup.BackupConfig{
					Partitions:     backup.NewPartitionRange(0, 4096),
					Set:            suite.set,
					Namespace:      suite.namespace,
					Parallel:       100,
					EncoderFactory: encoding.NewASBEncoderFactory(),
				},
				restoreConfig: backup.NewRestoreConfig(),
				bins:          testBins,
			},
		},
	}
	for _, tt := range tests {
		suite.SetupTest()
		suite.Run(tt.name, func() {
			runBackupRestoreDirectory(suite, tt.args.backupConfig, tt.args.restoreConfig, tt.args.bins)
		})
		suite.TearDownTest()
	}
}

func runBackupRestoreDirectory(suite *backupRestoreTestSuite,
	backupConfig *backup.BackupConfig,
	restoreConfig *backup.RestoreConfig,
	bins a.BinMap) {
	numRec := 1000
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	backupDir := suite.T().TempDir()
	writerFactory, _ := backup.NewDirectoryWriterFactory(backupDir, 0, backupConfig.EncoderFactory)

	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		writerFactory,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	statsBackup := bh.GetStats()
	suite.NotNil(statsBackup)

	err = bh.Wait(ctx)
	suite.Nil(err)

	suite.Equal(uint64(numRec), statsBackup.GetRecords())
	suite.Equal(uint32(0), statsBackup.GetSIndexes())
	suite.Equal(uint32(0), statsBackup.GetUDFs())

	backupFiles, _ := os.ReadDir(backupDir)
	suite.Equal(backupConfig.Parallel, len(backupFiles))

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		suite.FailNow(err.Error())
	}

	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		backup.NewFileReaderFactory(backupDir, restoreConfig.DecoderFactory),
	)
	suite.Nil(err)

	statsRestore := rh.GetStats()
	suite.NotNil(statsRestore)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.Equal(uint64(numRec), statsRestore.GetRecords())
	suite.Equal(uint32(0), statsRestore.GetSIndexes())
	suite.Equal(uint32(0), statsRestore.GetUDFs())
	suite.Equal(uint64(0), statsRestore.GetRecordsExpired())
}

func (suite *backupRestoreTestSuite) TestRestoreExpiredRecords() {
	numRec := 100
	bins := a.BinMap{
		"IntBin": 1,
	}
	recs := genRecords(suite.namespace, suite.set, numRec, bins)

	data := &bytes.Buffer{}
	encoder, err := asb.NewEncoder()
	if err != nil {
		suite.FailNow(err.Error())
	}

	header, err := asb.GetHeader(suite.namespace, true)
	if err != nil {
		suite.FailNow(err.Error())
	}

	data.Write(header)

	for _, rec := range recs {
		modelRec := models.Record{
			Record: rec,
			// guaranteed to be expired
			VoidTime: 1,
		}

		token := models.NewRecordToken(modelRec)
		v, err := encoder.EncodeToken(token)
		if err != nil {
			suite.FailNow(err.Error())
		}

		_, err = data.Write(v)
		if err != nil {
			suite.FailNow(err.Error())
		}
	}

	ctx := context.Background()
	reader := &ByteReaderWriterFactory{
		bytes.NewBuffer(data.Bytes()),
	}
	rh, err := suite.backupClient.Restore(
		ctx,
		backup.NewRestoreConfig(),
		reader,
	)
	suite.Nil(err)
	suite.NotNil(rh)

	restoreStats := rh.GetStats()
	suite.NotNil(restoreStats)

	err = rh.Wait(ctx)
	suite.Nil(err)

	statsRestore := rh.GetStats()
	suite.NotNil(statsRestore)
	suite.Equal(uint64(0), statsRestore.GetRecords())
	suite.Equal(uint64(numRec), statsRestore.GetRecordsExpired())
}

func (suite *backupRestoreTestSuite) TestBackupRestoreIOWithPartitions() {
	numRec := 1000
	bins := a.BinMap{
		"IntBin": 1,
	}
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
	if err != nil {
		panic(err)
	}

	recsByPartition := make(map[int][]*a.Record)

	for _, rec := range expectedRecs {
		partitionID := rec.Key.PartitionId()

		if _, ok := recsByPartition[partitionID]; !ok {
			recsByPartition[partitionID] = []*a.Record{}
		}

		recsByPartition[partitionID] = append(recsByPartition[partitionID], rec)
	}

	// backup half the partitions
	startPartition := 256
	partitionCount := 2056
	partitions := backup.NewPartitionRange(startPartition, partitionCount)

	// reset the expected record count
	numRec = 0

	expectedRecs = []*a.Record{}
	for pid, recs := range recsByPartition {
		if pid >= startPartition && pid < startPartition+partitionCount {
			numRec += len(recs)
			expectedRecs = append(expectedRecs, recs...)
		}
	}

	ctx := context.Background()

	backupConfig := backup.NewBackupConfig()
	backupConfig.Partitions = partitions

	backupDir := suite.T().TempDir()
	writerFactory, _ := backup.NewDirectoryWriterFactory(backupDir, 0, backupConfig.EncoderFactory)
	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		writerFactory,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	err = bh.Wait(ctx)
	suite.Nil(err)

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		panic(err)
	}

	restoreConfig := backup.NewRestoreConfig()
	readerFactory := backup.NewFileReaderFactory(backupDir, restoreConfig.DecoderFactory)

	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		readerFactory,
	)
	suite.Nil(err)
	suite.NotNil(rh)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.testClient.ValidateRecords(suite.T(), expectedRecs, numRec, suite.namespace, suite.set)
}

func (suite *backupRestoreTestSuite) TestBackupContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writer := ByteReaderWriterFactory{}
	bh, err := suite.backupClient.Backup(
		ctx,
		backup.NewBackupConfig(),
		&writer,
	)
	suite.NotNil(bh)
	suite.Nil(err)

	ctx = context.Background()
	err = bh.Wait(ctx)
	suite.NotNil(err)
}

func (suite *backupRestoreTestSuite) TestRestoreContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	restoreConfig := backup.NewRestoreConfig()
	reader := ByteReaderWriterFactory{buffer: bytes.NewBuffer([]byte{})}
	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		&reader,
	)
	suite.NotNil(rh)
	suite.Nil(err)

	ctx = context.Background()
	err = rh.Wait(ctx)
	suite.NotNil(err)
}

func genRecords(namespace, set string, numRec int, bins a.BinMap) []*a.Record {
	userKeys := []any{1, "string", []byte("bytes")}
	recs := make([]*a.Record, numRec)
	for i := 0; i < numRec; i++ {
		userKey := userKeys[i%len(userKeys)]
		switch k := userKey.(type) {
		case int:
			userKey = i
		case string:
			userKey = k + fmt.Sprint(i)
		case []byte:
			k = append(k, []byte(fmt.Sprint(i))...)
			userKey = k
		}
		key, err := a.NewKey(namespace, set, userKey)
		if err != nil {
			panic(err)
		}
		recs[i] = &a.Record{
			Key:  key,
			Bins: bins,
		}
	}
	return recs
}

func TestBackupRestoreTestSuite(t *testing.T) {
	testSuite := backupRestoreTestSuite{
		aerospikeIP:       testutils.IP,
		aerospikePort:     testutils.PortStart,
		aerospikePassword: testutils.Password,
		aerospikeUser:     testutils.User,
		namespace:         "test",
		set:               "",
	}

	suite.Run(t, &testSuite)
}

type ByteReaderWriterFactory struct {
	buffer *bytes.Buffer
}

func (b *ByteReaderWriterFactory) Readers() ([]io.ReadCloser, error) {
	reader := io.NopCloser(bytes.NewReader(b.buffer.Bytes()))
	return []io.ReadCloser{reader}, nil
}

type nopWriteCloser struct {
	sync.Mutex
	*bytes.Buffer
}

func (n *nopWriteCloser) Close() error {
	return nil
}

func (n *nopWriteCloser) Write(p []byte) (int, error) {
	n.Lock()
	defer n.Unlock()
	return n.Buffer.Write(p)
}

func (b *ByteReaderWriterFactory) NewWriter(_ string) (io.WriteCloser, error) {
	return &nopWriteCloser{sync.Mutex{}, b.buffer}, nil
}
