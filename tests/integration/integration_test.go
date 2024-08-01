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
	"fmt"
	"io"
	"log/slog"
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

	backupClient, err := backup.NewClient(testAeroClient, "test_client", slog.Default())
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
	var tests = []struct {
		args args
		name string
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
	nonBatchRestore := backup.NewRestoreConfig()
	nonBatchRestore.DisableBatchWrites = true
	batchRestore := backup.NewRestoreConfig()
	batchRestore.DisableBatchWrites = false
	configWithFileLimit := backup.NewBackupConfig()
	configWithFileLimit.FileLimit = 1024 * 1024

	var tests = []struct {
		name string
		args args
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewBackupConfig(),
				restoreConfig: nonBatchRestore,
				bins:          testBins,
				expectedFiles: 1,
			},
		},
		{
			name: "default batch",
			args: args{
				backupConfig:  backup.NewBackupConfig(),
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
					Partitions:  backup.PartitionRangeAll(),
					SetList:     []string{suite.set},
					Namespace:   suite.namespace,
					Parallel:    100,
					EncoderType: backup.EncoderTypeASB,
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
					Partitions:  backup.PartitionRangeAll(),
					SetList:     []string{suite.set},
					Namespace:   suite.namespace,
					Parallel:    2,
					EncoderType: backup.EncoderTypeASB,
					FileLimit:   3 * 1024 * 1024, // 3mb, full backup ~9mb
				},
				restoreConfig: nonBatchRestore,
				bins:          testBins,
				expectedFiles: 4, // every thread create one big and one small file
			},
		},
	}
	for _, tt := range tests {
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
	writerFactory, err := local.NewDirectoryWriterFactory(backupDir, false)
	suite.Nil(err)

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

	streamingReader, _ := backup.NewStreamingReaderLocal(backupDir, backupConfig.EncoderType)
	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		streamingReader,
	)
	suite.Nil(err)

	statsRestore := rh.GetStats()
	suite.NotNil(statsRestore)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.Require().Equal(uint64(len(expectedRecs)), statsRestore.GetReadRecords())
	suite.Require().Equal(uint64(len(expectedRecs)), statsRestore.GetRecordsInserted())
	suite.Require().Equal(uint32(8), statsRestore.GetSIndexes())
	suite.Require().Equal(uint32(3), statsRestore.GetUDFs())
	suite.Require().Equal(uint64(0), statsRestore.GetRecordsExpired())
	suite.Require().Less(statsRestore.GetTotalBytesRead(), dirSize) // restore size doesn't include asb control characters

	suite.testClient.ValidateSIndexes(suite.T(), suite.expectedSIndexes, suite.namespace)
	suite.testClient.ValidateRecords(suite.T(), expectedRecs, suite.namespace, suite.set)

	_, err = local.NewDirectoryWriterFactory(backupDir, false)
	suite.ErrorContains(err, "is not empty")
}

func (suite *backupRestoreTestSuite) TestRestoreExpiredRecords() {
	numRec := 100
	bins := a.BinMap{
		"IntBin": 1,
	}
	recs := genRecords(suite.namespace, suite.set, numRec, bins)

	data := &bytes.Buffer{}
	encoder := asb.NewEncoder("test")

	header := encoder.GetHeader()

	data.Write(header)

	for _, rec := range recs {
		modelRec := models.Record{
			Record: rec,
			// guaranteed to be expired
			VoidTime: 1,
		}

		token := models.NewRecordToken(modelRec, 0)
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
	writerFactory, _ := local.NewDirectoryWriterFactory(backupDir, false)
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
	streamingReader, _ := backup.NewStreamingReaderLocal(backupDir, backupConfig.EncoderType)

	rh, err := suite.backupClient.Restore(
		ctx,
		restoreConfig,
		streamingReader,
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
	reader := byteReadWriterFactory{buffer: bytes.NewBuffer([]byte{})}
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
		Partitions:  backup.PartitionRangeAll(),
		SetList:     []string{suite.set},
		Namespace:   suite.namespace,
		Parallel:    1,
		EncoderType: backup.EncoderTypeASB,
		BinList:     []string{"BackupRestore", "OnlyBackup"},
	}

	var restoreConfig = backup.NewRestoreConfig()
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
		Partitions:  backup.PartitionRangeAll(),
		SetList:     []string{suite.set},
		Namespace:   suite.namespace,
		Parallel:    1,
		EncoderType: backup.EncoderTypeASB,
		ModAfter:    &lowerLimit,
		ModBefore:   &upperLimit,
	}

	var restoreConfig = backup.NewRestoreConfig()

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
		Partitions:  backup.PartitionRangeAll(),
		SetList:     []string{suite.set},
		Namespace:   suite.namespace,
		Parallel:    1,
		EncoderType: backup.EncoderTypeASB,
	}
	backupConfig.ScanPolicy = suite.Aeroclient.DefaultScanPolicy
	backupConfig.ScanPolicy.RecordsPerSecond = rps

	var restoreConfig = backup.NewRestoreConfig()
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

type byteReadWriterFactory struct {
	buffer *bytes.Buffer
}

var _ backup.Writer = (*byteReadWriterFactory)(nil)

func (b *byteReadWriterFactory) StreamFiles(_ context.Context, readersCh chan<- io.ReadCloser, _ chan<- error) {
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
