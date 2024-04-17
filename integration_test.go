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
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	backup "github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	testresources "github.com/aerospike/backup-go/internal/testutils"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/testutils"
	testSuite "github.com/stretchr/testify/suite"
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
	testSuite.Suite
	aerospikeIP       string
	aerospikePort     int
	aerospikePassword string
	aerospikeUser     string
	namespace         string
	set               string
	Aeroclient        *a.Client
	testClient        *testresources.TestClient
	backupClient      *backup.Client
	expectedSIndexes  []*models.SIndex
	expectedUDFs      []*models.UDF
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
		{
			name: "with parallel backup",
			args: args{
				backupConfig: &backup.BackupConfig{
					Partitions:     backup.NewPartitionRange(0, 4096),
					Set:            suite.set,
					Namespace:      suite.namespace,
					Parallel:       4,
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
			runBackupRestore(suite, tt.args.backupConfig, tt.args.restoreConfig, tt.args.bins)
		})
		suite.TearDownTest()
	}
}

func runBackupRestore(suite *backupRestoreTestSuite, backupConfig *backup.BackupConfig,
	restoreConfig *backup.RestoreConfig, bins a.BinMap) {
	numRec := 1000
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
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

	ctx := context.Background()
	dst := bytes.NewBuffer([]byte{})

	bh, err := suite.backupClient.Backup(
		ctx,
		[]io.Writer{dst},
		backupConfig,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	err = bh.Wait(ctx)
	suite.Nil(err)

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		panic(err)
	}

	reader := bytes.NewReader(dst.Bytes())

	rh, err := suite.backupClient.Restore(
		ctx,
		[]io.Reader{reader},
		restoreConfig,
	)
	suite.Nil(err)
	suite.NotNil(rh)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.testClient.ValidateRecords(suite.T(), expectedRecs, numRec, suite.namespace, suite.set)
	suite.testClient.ValidateSIndexes(suite.T(), suite.expectedSIndexes, suite.namespace)
}

func (suite *backupRestoreTestSuite) TestBackupRestoreDirectory() {
	type args struct {
		backupConfig  *backup.BackupToDirectoryConfig
		restoreConfig *backup.RestoreFromDirectoryConfig
		bins          a.BinMap
	}
	var tests = []struct {
		name string
		args args
	}{
		{
			name: "default",
			args: args{
				backupConfig:  backup.NewBackupToDirectoryConfig(),
				restoreConfig: backup.NewRestoreFromDirectoryConfig(),
				bins:          testBins,
			},
		},
		{
			name: "with file size limit",
			args: args{
				backupConfig: &backup.BackupToDirectoryConfig{
					FileSizeLimit: 1024 * 1024,
					BackupConfig: backup.BackupConfig{
						Partitions:     backup.NewPartitionRange(0, 4096),
						Set:            suite.set,
						Namespace:      suite.namespace,
						Parallel:       4,
						EncoderFactory: encoding.NewASBEncoderFactory(),
					},
				},
				restoreConfig: backup.NewRestoreFromDirectoryConfig(),
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
	backupConfig *backup.BackupToDirectoryConfig,
	restoreConfig *backup.RestoreFromDirectoryConfig,
	bins a.BinMap) {
	numRec := 1000
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
	if err != nil {
		panic(err)
	}

	err = suite.testClient.WriteSIndexes(suite.expectedSIndexes)
	if err != nil {
		suite.FailNow(err.Error())
	}

	err = suite.testClient.WriteUDFs(suite.expectedUDFs)
	if err != nil {
		suite.FailNow(err.Error())
	}

	ctx := context.Background()

	backupDir := suite.T().TempDir()

	bh, err := suite.backupClient.BackupToDirectory(
		ctx,
		backupDir,
		backupConfig,
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

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		suite.FailNow(err.Error())
	}

	rh, err := suite.backupClient.RestoreFromDirectory(
		ctx,
		backupDir,
		restoreConfig,
	)
	suite.Nil(err)

	statsRestore := rh.GetStats()
	suite.NotNil(statsRestore)

	err = rh.Wait(ctx)
	suite.Nil(err)

	suite.Equal(uint64(numRec), statsRestore.GetRecords())
	suite.Equal(uint32(8), statsRestore.GetSIndexes())
	suite.Equal(uint32(3), statsRestore.GetUDFs())
	suite.Equal(uint64(0), statsRestore.GetRecordsExpired())

	suite.testClient.ValidateSIndexes(suite.T(), suite.expectedSIndexes, suite.namespace)
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
	reader := bytes.NewReader(data.Bytes())
	rh, err := suite.backupClient.Restore(
		ctx,
		[]io.Reader{reader},
		nil,
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
	dst := bytes.NewBuffer([]byte{})

	backupConfig := backup.NewBackupConfig()
	backupConfig.Partitions = partitions

	bh, err := suite.backupClient.Backup(
		ctx,
		[]io.Writer{dst},
		backupConfig,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	err = bh.Wait(ctx)
	suite.Nil(err)

	err = suite.testClient.Truncate(suite.namespace, suite.set)
	if err != nil {
		panic(err)
	}

	reader := bytes.NewReader(dst.Bytes())

	rh, err := suite.backupClient.Restore(
		ctx,
		[]io.Reader{reader},
		nil,
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

	dst := bytes.NewBuffer([]byte{})

	bh, err := suite.backupClient.Backup(
		ctx,
		[]io.Writer{dst},
		nil,
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

	reader := bytes.NewReader([]byte{})

	rh, err := suite.backupClient.Restore(
		ctx,
		[]io.Reader{reader},
		nil,
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
			Content: []byte(testresources.UDF),
		},
		{
			Name:    "add.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte("function add(rec)\n  return 1 + 1\nend\n"),
		},
	}

	ts.expectedUDFs = expectedUDFs

	testSuite.Run(t, &ts)
}
