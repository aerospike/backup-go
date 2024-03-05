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

	testresources "github.com/aerospike/backup-go/test"

	backup "github.com/aerospike/backup-go"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/tools-common-go/testutils"
	"github.com/stretchr/testify/suite"
)

const (
	// got this from hllop := a.HLLAddOp(hllpol, "hll", []a.Value{a.NewIntegerValue(1)}, 4, 12)
	//nolint:lll // can't split this up without making it a raw quote which will cause the escaped bytes to be interpreted literally
	hllValue = "\x00\x04\f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x84\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
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
		"HLLBin":     a.NewHLLValue([]byte(hllValue)),
		"MapBin": map[any]any{
			"IntBin":    1,
			"StringBin": "hi",
			"listBin":   []any{1, 2, 3},
		},
		"ListBin": []any{
			1,
			"string",
			[]byte("bytes"),
			map[any]any{1: 1},
		},
	}
	expectedRecs := genRecords(suite.namespace, suite.set, numRec, bins)

	err := suite.testClient.WriteRecords(expectedRecs)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	dst := bytes.NewBuffer([]byte{})

	bh, err := suite.backupClient.Backup(
		ctx,
		[]io.Writer{dst},
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
