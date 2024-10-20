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

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/local"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/tests"
	"github.com/aerospike/tools-common-go/testutils"
	"github.com/stretchr/testify/suite"
)

const (
	// got this from writing and reading back a.HLLAddOp(hllpol, "hll", []a.Value{a.NewIntegerValue(1)}, 4, 12)
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
	Aeroclient        *a.Client
	testClient        *tests.TestClient
	backupClient      *backup.Client
	aerospikeIP       string
	aerospikePassword string
	aerospikeUser     string
	namespace         string
	set               string
	expectedSIndexes  []*models.SIndex
	expectedUDFs      []*models.UDF
	aerospikePort     int
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
		{Code: a.SIndexAdmin},
		{Code: a.UDFAdmin},
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

	testClient := tests.NewTestClient(testAeroClient)
	suite.testClient = testClient

	backupClient, err := backup.NewClient(testAeroClient, backup.WithID("test_client"))
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

func (suite *backupRestoreTestSuite) SetupTest(records []*a.Record) {
	err := suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		suite.FailNow(err.Error())
	}

	err = suite.testClient.WriteRecords(records)
	if err != nil {
		suite.FailNow(err.Error())
	}

	err = suite.testClient.WriteSIndexes(suite.expectedSIndexes)
	if err != nil {
		suite.FailNow(err.Error())
	}

	err = suite.testClient.WriteUDFs(suite.expectedUDFs)
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
	var testsCases = []struct {
		args args
		name string
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewDefaultBackupConfig(),
				restoreConfig: backup.NewDefaultRestoreConfig(),
				bins:          testBins,
			},
		},
	}
	for _, tt := range testsCases {
		expectedRecs := genRecords(suite.namespace, suite.set, 1000, tt.args.bins)
		suite.SetupTest(expectedRecs)
		suite.Run(tt.name, func() {
			runBackupRestore(suite, tt.args.backupConfig, tt.args.restoreConfig, expectedRecs)
		})
		suite.TearDownTest()
	}
}

func runBackupRestore(suite *backupRestoreTestSuite, backupConfig *backup.BackupConfig,
	restoreConfig *backup.RestoreConfig, expectedRecs []*a.Record) (*models.BackupStats, *models.RestoreStats) {
	ctx := context.Background()
	dst := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}

	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		&dst,
		nil,
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

	suite.testClient.ValidateRecords(suite.T(), expectedRecs, suite.namespace, suite.set)
	suite.testClient.ValidateSIndexes(suite.T(), suite.expectedSIndexes, suite.namespace)
	return bh.GetStats(), rh.GetStats()
}

func (suite *backupRestoreTestSuite) TestBackupRestoreDirectory() {
	type args struct {
		backupConfig  *backup.BackupConfig
		restoreConfig *backup.RestoreConfig
		bins          a.BinMap
		expectedFiles int
	}
	nonBatchRestore := backup.NewDefaultRestoreConfig()
	nonBatchRestore.DisableBatchWrites = true
	batchRestore := backup.NewDefaultRestoreConfig()
	batchRestore.DisableBatchWrites = false
	configWithFileLimit := backup.NewDefaultBackupConfig()
	configWithFileLimit.FileLimit = 1024 * 1024

	var testsCases = []struct {
		name string
		args args
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewDefaultBackupConfig(),
				restoreConfig: nonBatchRestore,
				bins:          testBins,
				expectedFiles: 1,
			},
		},
		{
			name: "default batch",
			args: args{
				backupConfig:  backup.NewDefaultBackupConfig(),
				restoreConfig: batchRestore,
				bins:          testBins,
				expectedFiles: 1,
			},
		},
		{
			name: "with file size limit",
			args: args{
				backupConfig:  configWithFileLimit,
				restoreConfig: nonBatchRestore,
				bins:          testBins,
				expectedFiles: 10,
			},
		},
		{
			name: "with file size limit batch",
			args: args{
				backupConfig:  configWithFileLimit,
				restoreConfig: batchRestore,
				bins:          testBins,
				expectedFiles: 10,
			},
		},
		{
			name: "with parallel backup",
			args: args{
				backupConfig: &backup.BackupConfig{
					PartitionFilters: []*a.PartitionFilter{backup.NewPartitionFilterAll()},
					SetList:          []string{suite.set},
					Namespace:        suite.namespace,
					ParallelRead:     1,
					ParallelWrite:    100,
					EncoderType:      backup.EncoderTypeASB,
				},
				restoreConfig: nonBatchRestore,
				bins:          testBins,
				expectedFiles: 100,
			},
		},
		{
			name: "parallel with file size limit",
			args: args{
				backupConfig: &backup.BackupConfig{
					PartitionFilters: []*a.PartitionFilter{backup.NewPartitionFilterAll()},
					SetList:          []string{suite.set},
					Namespace:        suite.namespace,
					ParallelRead:     1,
					ParallelWrite:    2,
					EncoderType:      backup.EncoderTypeASB,
					FileLimit:        3 * 1024 * 1024, // 3mb, full backup ~9mb
				},
				restoreConfig: nonBatchRestore,
				bins:          testBins,
				expectedFiles: 4, // every thread create one big and one small file
			},
		},
	}
	for _, tt := range testsCases {
		var initialRecords = genRecords(suite.namespace, suite.set, 20_000, tt.args.bins)
		suite.SetupTest(initialRecords)
		suite.Run(tt.name, func() {
			runBackupRestoreDirectory(suite,
				tt.args.backupConfig, tt.args.restoreConfig, initialRecords, tt.args.expectedFiles)
		})
		suite.TearDownTest()
	}
}

func runBackupRestoreDirectory(suite *backupRestoreTestSuite,
	backupConfig *backup.BackupConfig,
	restoreConfig *backup.RestoreConfig,
	expectedRecs []*a.Record,
	expectedFiles int) {
	ctx := context.Background()

	backupDir := suite.T().TempDir()
	writers, err := local.NewWriter(
		ctx,
		local.WithValidator(asb.NewValidator()),
		local.WithDir(backupDir),
	)
	suite.Nil(err)

	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		writers,
		nil,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	statsBackup := bh.GetStats()
	suite.NotNil(statsBackup)

	err = bh.Wait(ctx)
	suite.Nil(err)

	suite.Require().Equal(uint64(len(expectedRecs)), statsBackup.GetReadRecords())
	suite.Require().Equal(uint32(8), statsBackup.GetSIndexes())
	suite.Require().Equal(uint32(3), statsBackup.GetUDFs())

	dirSize := uint64(tests.DirSize(backupDir))
	suite.Require().Equal(dirSize, statsBackup.GetBytesWritten())

	slog.Info("backup", "size", statsBackup.GetBytesWritten(), "files", tests.GetFileSizes(backupDir))

	backupFiles, _ := os.ReadDir(backupDir)
	suite.Require().Equal(expectedFiles, len(backupFiles))

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	suite.Nil(err)

	readers, err := local.NewReader(local.WithDir(backupDir))
	suite.Nil(err)
	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		readers,
	)
	suite.Nil(err)

	statsRestore := rh.GetStats()
	suite.NotNil(statsRestore)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.Require().EqualValues(uint64(len(expectedRecs)), statsRestore.GetReadRecords())
	suite.Require().Equal(uint64(len(expectedRecs)), statsRestore.GetRecordsInserted())
	suite.Require().Equal(uint32(8), statsRestore.GetSIndexes())
	suite.Require().Equal(uint32(3), statsRestore.GetUDFs())
	suite.Require().Equal(uint64(0), statsRestore.GetRecordsExpired())
	suite.Require().Less(statsRestore.GetTotalBytesRead(), dirSize) // restore size doesn't include asb control characters

	suite.testClient.ValidateSIndexes(suite.T(), suite.expectedSIndexes, suite.namespace)
	suite.testClient.ValidateRecords(suite.T(), expectedRecs, suite.namespace, suite.set)

	_, err = local.NewWriter(
		ctx,
		local.WithValidator(asb.NewValidator()),
		local.WithDir(backupDir),
	)
	suite.ErrorContains(err, "must be empty")
}

func (suite *backupRestoreTestSuite) TestRestoreExpiredRecords() {
	numRec := 100
	bins := a.BinMap{
		"IntBin": 1,
	}
	recs := genRecords(suite.namespace, suite.set, numRec, bins)

	data := &bytes.Buffer{}
	encoder := backup.NewEncoder(backup.EncoderTypeASB, "test", false)

	header := encoder.GetHeader()

	data.Write(header)

	for _, rec := range recs {
		modelRec := &models.Record{
			Record: rec,
			// guaranteed to be expired
			VoidTime: 1,
		}

		token := models.NewRecordToken(modelRec, 0, nil)
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
	reader := &byteReadWriterFactory{
		bytes.NewBuffer(data.Bytes()),
	}
	rh, err := suite.backupClient.Restore(
		ctx,
		backup.NewDefaultRestoreConfig(),
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
	suite.Equal(uint64(numRec), statsRestore.GetReadRecords())
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
	partitions := []*a.PartitionFilter{backup.NewPartitionFilterByRange(startPartition, partitionCount)}

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

	backupConfig := backup.NewDefaultBackupConfig()
	backupConfig.PartitionFilters = partitions

	backupDir := suite.T().TempDir()
	writers, err := local.NewWriter(
		ctx,
		local.WithValidator(asb.NewValidator()),
		local.WithDir(backupDir),
		local.WithRemoveFiles(),
	)
	suite.Require().NoError(err)
	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		writers,
		nil,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	err = bh.Wait(ctx)
	suite.Nil(err)

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		panic(err)
	}

	restoreConfig := backup.NewDefaultRestoreConfig()
	readers, err := local.NewReader(local.WithDir(backupDir))
	suite.Nil(err)

	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		readers,
	)
	suite.Nil(err)
	suite.NotNil(rh)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.testClient.ValidateRecords(suite.T(), expectedRecs, suite.namespace, suite.set)
}

func (suite *backupRestoreTestSuite) TestBackupContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writer := byteReadWriterFactory{}
	bh, err := suite.backupClient.Backup(
		ctx,
		backup.NewDefaultBackupConfig(),
		&writer,
		nil,
	)
	suite.NotNil(bh)
	suite.Nil(err)

	err = bh.Wait(ctx)
	suite.NotNil(err)
}

func (suite *backupRestoreTestSuite) TestRestoreContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	restoreConfig := backup.NewDefaultRestoreConfig()
	reader := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}
	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		&reader,
	)
	suite.NotNil(rh)
	suite.Nil(err)

	err = rh.Wait(ctx)
	suite.NotNil(err)
}

func (suite *backupRestoreTestSuite) TestBackupRestoreIOEncryptionFile() {
	type args struct {
		backupConfig  *backup.BackupConfig
		restoreConfig *backup.RestoreConfig
		bins          a.BinMap
	}

	privateKeyFile := "pkey_test"
	bCfg := backup.NewDefaultBackupConfig()
	bCfg.EncryptionPolicy = &backup.EncryptionPolicy{
		KeyFile: &privateKeyFile,
		Mode:    backup.EncryptAES128,
	}
	rCfg := backup.NewDefaultRestoreConfig()
	rCfg.EncryptionPolicy = &backup.EncryptionPolicy{
		KeyFile: &privateKeyFile,
		Mode:    backup.EncryptAES128,
	}

	var testsCases = []struct {
		args args
		name string
	}{
		{
			name: "default",
			args: args{
				backupConfig:  bCfg,
				restoreConfig: rCfg,
				bins:          testBins,
			},
		},
	}
	for _, tt := range testsCases {
		expectedRecs := genRecords(suite.namespace, suite.set, 1000, tt.args.bins)
		suite.SetupTest(expectedRecs)
		suite.Run(tt.name, func() {
			runBackupRestore(suite, tt.args.backupConfig, tt.args.restoreConfig, expectedRecs)
		})
		suite.TearDownTest()
	}
}

func (suite *backupRestoreTestSuite) TestBackupRestoreIONamespace() {
	type args struct {
		backupConfig  *backup.BackupConfig
		restoreConfig *backup.RestoreConfig
		bins          a.BinMap
	}

	source := "test"
	destination := "test"
	rCfg := backup.NewDefaultRestoreConfig()
	rCfg.Namespace = &backup.RestoreNamespaceConfig{
		Source:      &source,
		Destination: &destination,
	}

	var testsCases = []struct {
		args args
		name string
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewDefaultBackupConfig(),
				restoreConfig: rCfg,
				bins:          testBins,
			},
		},
	}
	for _, tt := range testsCases {
		expectedRecs := genRecords(suite.namespace, suite.set, 1000, tt.args.bins)
		suite.SetupTest(expectedRecs)
		suite.Run(tt.name, func() {
			runBackupRestore(suite, tt.args.backupConfig, tt.args.restoreConfig, expectedRecs)
		})
		suite.TearDownTest()
	}
}

func (suite *backupRestoreTestSuite) TestBackupRestoreIOCompression() {
	type args struct {
		backupConfig  *backup.BackupConfig
		restoreConfig *backup.RestoreConfig
		bins          a.BinMap
	}

	bCfg := backup.NewDefaultBackupConfig()
	bCfg.CompressionPolicy = backup.NewCompressionPolicy(backup.CompressZSTD, 20)
	rCfg := backup.NewDefaultRestoreConfig()
	rCfg.CompressionPolicy = backup.NewCompressionPolicy(backup.CompressZSTD, 20)

	var testsCases = []struct {
		args args
		name string
	}{
		{
			name: "default",
			args: args{
				backupConfig:  bCfg,
				restoreConfig: rCfg,
				bins:          testBins,
			},
		},
	}
	for _, tt := range testsCases {
		expectedRecs := genRecords(suite.namespace, suite.set, 1000, tt.args.bins)
		suite.SetupTest(expectedRecs)
		suite.Run(tt.name, func() {
			runBackupRestore(suite, tt.args.backupConfig, tt.args.restoreConfig, expectedRecs)
		})
		suite.TearDownTest()
	}
}

func (suite *backupRestoreTestSuite) TestBackupParallelNodes() {
	bCfg := backup.NewDefaultBackupConfig()
	bCfg.ParallelNodes = true

	ctx := context.Background()
	dst := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}
	bh, err := suite.backupClient.Backup(
		ctx,
		bCfg,
		&dst,
		nil,
	)
	suite.NotNil(bh)
	suite.Nil(err)
	err = bh.Wait(ctx)
	suite.Nil(err)
}

func (suite *backupRestoreTestSuite) TestBackupParallelNodesList() {
	bCfg := backup.NewDefaultBackupConfig()
	bCfg.NodeList = []string{fmt.Sprintf("%s:%d", suite.aerospikeIP, suite.aerospikePort)}

	ctx := context.Background()
	dst := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}
	bh, err := suite.backupClient.Backup(
		ctx,
		bCfg,
		&dst,
		nil,
	)
	suite.NotNil(bh)
	suite.Nil(err)
	err = bh.Wait(ctx)
	suite.Nil(err)
}

func (suite *backupRestoreTestSuite) TestBackupPartitionList() {
	batch := genRecords(suite.namespace, suite.set, 900, testBins)
	suite.SetupTest(batch)

	digest := base64.StdEncoding.EncodeToString(batch[0].Key.Digest())
	digestFilter, err := backup.NewPartitionFilterByDigest(suite.namespace, digest)
	suite.Nil(err)

	bCfg := backup.NewDefaultBackupConfig()
	bCfg.ParallelRead = 4
	bCfg.PartitionFilters = []*a.PartitionFilter{
		backup.NewPartitionFilterByID(1),
		backup.NewPartitionFilterByRange(2, 3),
		digestFilter,
	}

	ctx := context.Background()
	dst := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}
	bh, err := suite.backupClient.Backup(
		ctx,
		bCfg,
		&dst,
		nil,
	)
	suite.NotNil(bh)
	suite.Nil(err)
	err = bh.Wait(ctx)
	suite.Nil(err)
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
	ts := backupRestoreTestSuite{
		aerospikeIP:       testutils.IP,
		aerospikePort:     testutils.PortStart,
		aerospikePassword: testutils.Password,
		aerospikeUser:     testutils.User,
		namespace:         "test",
		set:               "",
	}

	listCtx, _ := a.CDTContextToBase64([]*a.CDTContext{a.CtxListValue(a.NewValue([]byte("hi")))})
	mapKeyCTX, _ := a.CDTContextToBase64([]*a.CDTContext{a.CtxMapKey(a.NewValue(1))})
	mapValueCTX, _ := a.CDTContextToBase64([]*a.CDTContext{a.CtxMapValue(a.NewValue("hi"))})

	expectedSIndexes := []*models.SIndex{
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "IntBinIndex",
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "IntBin",
				BinType: models.NumericSIDataType,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "StringBinIndex",
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "StringBin",
				BinType: models.StringSIDataType,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "ListBinIndex",
			IndexType: models.ListElementSIndex,
			Path: models.SIndexPath{
				BinName: "ListBin",
				BinType: models.NumericSIDataType,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "MapBinIndex",
			IndexType: models.MapKeySIndex,
			Path: models.SIndexPath{
				BinName: "MapBin",
				BinType: models.StringSIDataType,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "GeoJSONBinIndex",
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "GeoJSONBin",
				BinType: models.GEO2DSphereSIDataType,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "ListElemBinIndex",
			IndexType: models.ListElementSIndex,
			Path: models.SIndexPath{
				BinName:    "ListBin",
				BinType:    models.BlobSIDataType,
				B64Context: listCtx,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "MapKeyBinIndex",
			IndexType: models.MapKeySIndex,
			Path: models.SIndexPath{
				BinName:    "MapBin",
				BinType:    models.NumericSIDataType,
				B64Context: mapKeyCTX,
			},
		},
		{
			Namespace: ts.namespace,
			Set:       ts.set,
			Name:      "MapValBinIndex",
			IndexType: models.MapValueSIndex,
			Path: models.SIndexPath{
				BinName:    "MapBin",
				BinType:    models.StringSIDataType,
				B64Context: mapValueCTX,
			},
		},
	}

	ts.expectedSIndexes = expectedSIndexes

	expectedUDFs := []*models.UDF{
		{
			Name:    "simple_func.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte("function test(rec)\n  return 1\nend"),
		},
		{
			Name:    "test.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte(tests.UDF),
		},
		{
			Name:    "add.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte("function add(rec)\n  return 1 + 1\nend\n"),
		},
	}

	ts.expectedUDFs = expectedUDFs

	suite.Run(t, &ts)
}

func (suite *backupRestoreTestSuite) TestBinFilter() {
	var initialRecords = genRecords(suite.namespace, suite.set, 1000, a.BinMap{
		"BackupRestore": 1,
		"OnlyBackup":    2,
		"OnlyRestore":   3,
	})

	var expectedRecords = genRecords(suite.namespace, suite.set, 1000, a.BinMap{
		"BackupRestore": 1,
	})

	var backupConfig = &backup.BackupConfig{
		PartitionFilters: []*a.PartitionFilter{backup.NewPartitionFilterAll()},
		SetList:          []string{suite.set},
		Namespace:        suite.namespace,
		ParallelRead:     1,
		ParallelWrite:    1,
		EncoderType:      backup.EncoderTypeASB,
		BinList:          []string{"BackupRestore", "OnlyBackup"},
	}

	var restoreConfig = backup.NewDefaultRestoreConfig()
	restoreConfig.BinList = []string{"BackupRestore", "OnlyRestore"} // only BackupAndRestore should be restored

	suite.SetupTest(initialRecords)
	suite.Run("Filter by bin", func() {
		runBackupRestore(suite, backupConfig, restoreConfig, expectedRecords)
	})
	suite.TearDownTest()
}

func (suite *backupRestoreTestSuite) TestFilterTimestamp() {
	timeout := 1 * time.Second
	batch1 := genRecords(suite.namespace, suite.set, 900, testBins)
	suite.SetupTest(batch1)

	time.Sleep(timeout)
	lowerLimit := time.Now()
	batch2 := genRecords(suite.namespace, suite.set, 600, testBins)
	err := suite.testClient.WriteRecords(batch2)
	suite.Nil(err)

	time.Sleep(timeout)
	upperLimit := time.Now()
	time.Sleep(timeout)

	batch3 := genRecords(suite.namespace, suite.set, 300, testBins)
	err = suite.testClient.WriteRecords(batch3)
	suite.Nil(err)

	// every batch generated same records, but less of them each time.
	// batch1 contains too old values (many of them were overwritten).
	// batch3 contains too fresh values.
	var expectedRecords = tests.Subtract(batch2, batch3)

	var backupConfig = &backup.BackupConfig{
		PartitionFilters: []*a.PartitionFilter{backup.NewPartitionFilterAll()},
		SetList:          []string{suite.set},
		Namespace:        suite.namespace,
		ParallelRead:     1,
		ParallelWrite:    1,
		EncoderType:      backup.EncoderTypeASB,
		ModAfter:         &lowerLimit,
		ModBefore:        &upperLimit,
	}

	var restoreConfig = backup.NewDefaultRestoreConfig()

	suite.Run("Filter by bin", func() {
		runBackupRestore(suite, backupConfig, restoreConfig, expectedRecords)
	})
	suite.TearDownTest()
}

func (suite *backupRestoreTestSuite) TestRecordsPerSecond() {
	const numRec = 1000
	const rps = 200
	epsilon := 2 * float64(time.Second)

	records := genRecords(suite.namespace, suite.set, numRec, a.BinMap{"a": "b"})
	suite.SetupTest(records)

	var backupConfig = &backup.BackupConfig{
		PartitionFilters: []*a.PartitionFilter{backup.NewPartitionFilterAll()},
		SetList:          []string{suite.set},
		Namespace:        suite.namespace,
		ParallelRead:     1,
		ParallelWrite:    1,
		EncoderType:      backup.EncoderTypeASB,
	}
	backupConfig.ScanPolicy = suite.Aeroclient.DefaultScanPolicy
	backupConfig.ScanPolicy.RecordsPerSecond = rps

	var restoreConfig = backup.NewDefaultRestoreConfig()
	restoreConfig.RecordsPerSecond = rps

	now := time.Now()
	backupStats, restoreStats := runBackupRestore(suite, backupConfig, restoreConfig, records)
	totalDuration := time.Since(now)

	expectedDuration := time.Duration(1000.0*numRec/rps) * time.Millisecond
	suite.Require().InDelta(expectedDuration, backupStats.GetDuration(), epsilon)
	suite.Require().InDelta(expectedDuration, restoreStats.GetDuration(), epsilon)
	suite.Require().InDelta(totalDuration, restoreStats.GetDuration()+backupStats.GetDuration(), epsilon)

	suite.TearDownTest()
}

func (suite *backupRestoreTestSuite) TestBackupAfterDigestOk() {
	batch := genRecords(suite.namespace, suite.set, 900, testBins)
	suite.SetupTest(batch)

	digest := base64.StdEncoding.EncodeToString(batch[0].Key.Digest())
	afterDigestFilter, err := backup.NewPartitionFilterAfterDigest(suite.namespace, digest)
	suite.Nil(err)

	var backupConfig = &backup.BackupConfig{
		PartitionFilters: []*a.PartitionFilter{afterDigestFilter},
		SetList:          []string{suite.set},
		Namespace:        suite.namespace,
		ParallelRead:     1,
		ParallelWrite:    1,
		EncoderType:      backup.EncoderTypeASB,
	}

	ctx := context.Background()
	dst := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}
	bh, err := suite.backupClient.Backup(
		ctx,
		backupConfig,
		&dst,
		nil,
	)
	suite.Nil(err)
	suite.NotNil(bh)
	suite.TearDownTest()
}

func (suite *backupRestoreTestSuite) TestBackupEstimateOk() {
	batch := genRecords(suite.namespace, suite.set, 100, testBins)
	suite.SetupTest(batch)

	backupConfig := backup.NewDefaultBackupConfig()
	ctx := context.Background()
	bh, err := suite.backupClient.Estimate(
		ctx,
		backupConfig,
		10,
	)
	suite.Nil(err)
	suite.NotNil(bh)
	suite.TearDownTest()
}

func (suite *backupRestoreTestSuite) TestBackupContinuation() {
	const totalRecords = 900
	batch := genRecords(suite.namespace, suite.set, totalRecords, testBins)
	suite.SetupTest(batch)

	testFolder := suite.T().TempDir()
	testFile := "test_state_file"

	for i := 0; i < 5; i++ {
		randomNumber := rand.Intn(7-3+1) + 3
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Duration(randomNumber) * time.Second)
			cancel()
		}()

		first := suite.runFirstBackup(ctx, testFolder, testFile, i)

		ctx = context.Background()
		second := suite.runContinueBackup(ctx, testFolder, testFile, i)

		suite.T().Log("first:", first, "second:", second)
		result := (first + second) >= totalRecords
		suite.T().Log(first + second)
		suite.Equal(true, result)
	}

	suite.TearDownTest()
}

func (suite *backupRestoreTestSuite) runFirstBackup(ctx context.Context, testFolder, testStateFile string, i int,
) uint64 {
	bFolder := fmt.Sprintf("%s_%d", testFolder, i)

	writers, err := local.NewWriter(
		ctx,
		local.WithValidator(asb.NewValidator()),
		local.WithSkipDirCheck(),
		local.WithDir(bFolder),
	)
	if err != nil {
		panic(err)
	}

	readers, err := local.NewReader(
		local.WithDir(bFolder),
	)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = suite.namespace
	backupCfg.ParallelRead = 10
	backupCfg.ParallelWrite = 10

	backupCfg.StateFile = testStateFile
	backupCfg.FileLimit = 100000
	backupCfg.Bandwidth = 1000000
	backupCfg.PageSize = 100
	backupCfg.SyncPipelines = true

	backupHandler, err := suite.backupClient.Backup(ctx, backupCfg, writers, readers)
	if err != nil {
		panic(err)
	}

	// use backupHandler.Wait() to wait for the job to finish or fail
	err = backupHandler.Wait(ctx)
	if err != nil {
		suite.T().Logf("Backup failed: %v", err)
	}

	return backupHandler.GetStats().GetReadRecords()
}

func (suite *backupRestoreTestSuite) runContinueBackup(ctx context.Context, testFolder, testStateFile string, i int,
) uint64 {
	bFolder := fmt.Sprintf("%s_%d", testFolder, i)

	writers, err := local.NewWriter(
		ctx,
		local.WithValidator(asb.NewValidator()),
		local.WithSkipDirCheck(),
		local.WithDir(bFolder),
	)
	if err != nil {
		panic(err)
	}

	readers, err := local.NewReader(
		local.WithDir(bFolder),
	)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = suite.namespace
	backupCfg.ParallelRead = 10
	backupCfg.ParallelWrite = 10

	backupCfg.StateFile = testStateFile
	backupCfg.Continue = true
	backupCfg.FileLimit = 100000
	backupCfg.PageSize = 100
	backupCfg.SyncPipelines = true

	backupHandler, err := suite.backupClient.Backup(ctx, backupCfg, writers, readers)
	if err != nil {
		panic(err)
	}

	// use backupHandler.Wait() to wait for the job to finish or fail
	err = backupHandler.Wait(ctx)
	if err != nil {
		suite.T().Logf("Backup failed: %v", err)
	}

	return backupHandler.GetStats().GetReadRecords()
}

type byteReadWriterFactory struct {
	buffer *bytes.Buffer
}

func (b *byteReadWriterFactory) StreamFiles(_ context.Context, readersCh chan<- io.ReadCloser, _ chan<- error) {
	reader := io.NopCloser(bytes.NewReader(b.buffer.Bytes()))
	readersCh <- reader
	close(readersCh)
}

func (b *byteReadWriterFactory) StreamFile(_ context.Context, _ string, readersCh chan<- io.ReadCloser, _ chan<- error) {
	reader := io.NopCloser(bytes.NewReader(b.buffer.Bytes()))
	readersCh <- reader
	close(readersCh)
}

func (b *byteReadWriterFactory) OpenFile(_ context.Context, _ string, readersCh chan<- io.ReadCloser, _ chan<- error) {
	reader := io.NopCloser(bytes.NewReader(b.buffer.Bytes()))
	readersCh <- reader
	close(readersCh)
}

func (b *byteReadWriterFactory) Readers() ([]io.ReadCloser, error) {
	reader := io.NopCloser(bytes.NewReader(b.buffer.Bytes()))
	return []io.ReadCloser{reader}, nil
}

func (b *byteReadWriterFactory) GetType() string {
	return "byte buffer"
}

func (b *byteReadWriterFactory) RemoveFiles(_ context.Context) error {
	return nil
}

type nopWriteCloser struct {
	*bytes.Buffer
}

func (n *nopWriteCloser) Close() error {
	return nil
}

func (n *nopWriteCloser) Write(p []byte) (int, error) {
	return n.Buffer.Write(p)
}

func (b *byteReadWriterFactory) NewWriter(_ context.Context, _ string) (
	io.WriteCloser, error) {
	buffer := &nopWriteCloser{b.buffer}
	return buffer, nil
}
