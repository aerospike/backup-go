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

package backuplib_test

import (
	"backuplib"
	testresources "backuplib/test_resources"
	"bytes"
	"fmt"
	"io"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

const (
	Host          = "127.0.0.1"
	Port          = 3000
	namespace     = "test"
	set           = ""
	BackupDirPath = "./test_resources/backup"
)

type backupRestoreTestSuite struct {
	suite.Suite
	Aeroclient   *a.Client
	testClient   *testresources.TestClient
	backupClient *backuplib.Client
}

func (suite *backupRestoreTestSuite) SetupSuite() {
	// TODO update testutils to use v7 go client
	// testutils.Image = "aerospike/aerospike-server-enterprise:7.0.0.2"

	// clusterSize := 1
	// testutils.Start(clusterSize)

	asc, aerr := a.NewClientWithPolicy(
		a.NewClientPolicy(),
		Host,
		Port,
	)

	if aerr != nil {
		panic(aerr)
	}

	suite.Aeroclient = asc

	testClient := testresources.NewTestClient(asc)
	suite.testClient = testClient

	backupCFG := backuplib.Config{}
	backupClient, err := backuplib.NewClient(asc, backupCFG)
	if err != nil {
		panic(err)
	}

	suite.backupClient = backupClient
}

func (suite *backupRestoreTestSuite) TearDownSuite() {
	suite.Aeroclient.Close()
	// testutils.Stop()
}

func (suite *backupRestoreTestSuite) SetupTest() {
	err := suite.testClient.Truncate(namespace, set)
	if err != nil {
		panic(err)
	}
}

func (suite *backupRestoreTestSuite) TearDownTest() {
	err := suite.testClient.Truncate(namespace, set)
	if err != nil {
		panic(err)
	}
}

func (suite *backupRestoreTestSuite) TestBackupRestoreIO() {
	numRec := 1000
	bins := a.BinMap{
		"IntBin":    1,
		"FloatBin":  1.1,
		"StringBin": "string",
		"BoolBin":   true,
		"BlobBin":   []byte("bytes"),
		// TODO "HLLBin":    a.NewHLLValue([]byte{}),
		// TODO "MapBin": a.NewList(1, "string", true, []byte("bytes")),
		// TODO "ListBin": a.NewMap(map[interface{}]interface{}{}),
	}
	expectedRecs := genRecords(namespace, set, numRec, bins)

	err := suite.testClient.WriteRecords(namespace, set, expectedRecs)
	if err != nil {
		panic(err)
	}

	dst := bytes.NewBuffer([]byte{})

	bh, err := suite.backupClient.BackupToWriter(
		[]io.Writer{dst},
		nil,
	)
	suite.Nil(err)
	suite.NotNil(bh)

	err = bh.Wait()
	suite.Nil(err)

	err = suite.testClient.Truncate(namespace, set)
	if err != nil {
		panic(err)
	}

	reader := bytes.NewReader(dst.Bytes())

	rh, err := suite.backupClient.RestoreFromReader(
		[]io.Reader{reader},
		nil,
	)
	suite.Nil(err)
	suite.NotNil(rh)

	err = rh.Wait()
	suite.Nil(err)

	err = suite.testClient.ValidateRecords(expectedRecs, numRec, namespace, set)
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
			userKey = append(k, []byte(fmt.Sprint(i))...)
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
	suite.Run(t, new(backupRestoreTestSuite))
}
