package backuplib_test

import (
	"backuplib"
	testresources "backuplib/test_resources"
	"bytes"
	"io"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

const (
	Host          = "127.0.0.1"
	Port          = 3000
	namespace     = "test"
	BackupDirPath = "./test_resources/backup"
)

type backupRestoreTestSuite struct {
	suite.Suite
	Aeroclient   *a.Client
	testClient   *testresources.TestClient
	backupClient *backuplib.Client
}

func (suite *backupRestoreTestSuite) SetupSuite() {
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
}

func (suite *backupRestoreTestSuite) SetupTest() {
	suite.testClient.Truncate(namespace, "")
}

func (suite *backupRestoreTestSuite) TearDownTest() {
	suite.testClient.Truncate(namespace, "")
}

// TODO make sure that sindex NULL values are filtered out
// TODO mock the DB
func (suite *backupRestoreTestSuite) TestBackupToWriter() {
	// Write some records to the database
	numRec := 100
	expectedRecs := make([]*a.Record, numRec)
	for i := 0; i < numRec; i++ {
		key, err := a.NewKey(namespace, "", i)
		if err != nil {
			panic(err)
		}
		expectedRecs[i] = &a.Record{
			Key: key,
			Bins: a.BinMap{
				"bin1": i,
			},
		}
	}

	err := suite.testClient.WriteRecords(numRec, namespace, "", expectedRecs)
	if err != nil {
		panic(err)
	}

	dst := bytes.NewBuffer([]byte{})
	enc := backuplib.NewASBEncoderFactory()

	bh, errors := suite.backupClient.BackupToWriter(
		[]io.Writer{dst},
		enc,
		"test",
		backuplib.BackupToWriterOptions{
			Parallel: 1,
		},
	)
	suite.Assert().NotNil(bh)

	err = <-errors
	suite.Assert().Nil(err)

	suite.testClient.Truncate(namespace, "")

	reader := bytes.NewReader(dst.Bytes())
	dec := backuplib.NewASBDecoderBuilder()

	rh, errors := suite.backupClient.RestoreFromReader(
		[]io.Reader{reader},
		dec,
		backuplib.RestoreFromReaderOptions{
			Parallel: 1,
		},
	)
	suite.Assert().NotNil(rh)

	err = <-errors
	suite.Assert().Nil(err)

	err = suite.testClient.ValidateRecords(expectedRecs, numRec, namespace, "")
	suite.Assert().Nil(err)

}

func TestBackupRestoreTestSuite(t *testing.T) {
	suite.Run(t, new(backupRestoreTestSuite))
}
