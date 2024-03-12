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
	"testing"

	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/models"
	testresources "github.com/aerospike/backup-go/test"

	backup "github.com/aerospike/backup-go"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/tools-common-go/testutils"
	"github.com/stretchr/testify/suite"
)

const (
	BackupDirPath = "./test_resources/backup"
)

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
	backupClient, err := backup.NewClient(testAeroClient, &backupCFG)
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
	numRec := 1000
	bins := a.BinMap{
		"IntBin":     1,
		"FloatBin":   1.1,
		"StringBin":  "string",
		"BoolBin":    true,
		"BlobBin":    []byte("bytes"),
		"GeoJSONBin": a.GeoJSONValue(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
		// TODO "HLLBin":    a.NewHLLValue([]byte{}),
		// TODO "MapBin": a.NewList(1, "string", true, []byte("bytes")),
		// TODO "ListBin": a.NewMap(map[interface{}]interface{}{}),
	}
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	dst := bytes.NewBuffer([]byte{})

	backupConfig := backup.NewBackupConfig()

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

	err = suite.testClient.ValidateRecords(expectedRecs, numRec, suite.namespace, suite.set)
	suite.Nil(err)

	// validate backup statsBackup
	statsBackup := bh.GetStats()
	suite.NotNil(statsBackup)

	suite.Equal(uint64(numRec), statsBackup.GetRecords())
	suite.Equal(uint32(0), statsBackup.GetSIndexes())
	suite.Equal(uint32(0), statsBackup.GetUDFs())

	// validate stats for restore
	statsRestore := rh.GetStats()
	suite.NotNil(statsRestore)

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

	data.Write(encoder.GetVersionText())
	data.Write(encoder.GetNamespaceMetaText(suite.namespace))
	data.Write(encoder.GetFirstMetaText())

	for _, rec := range recs {
		modelRec := models.Record{
			Record: rec,
			// guaranteed to be expired
			VoidTime: 1,
		}
		v, err := encoder.EncodeRecord(&modelRec)
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

	err = suite.testClient.ValidateRecords(expectedRecs, numRec, suite.namespace, suite.set)
	suite.Nil(err)
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
