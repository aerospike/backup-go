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
	BackupDirPath = "./test_resources/backup"
)

type BackupRestoreTestSuite struct {
	suite.Suite
	Aeroclient   *a.Client
	testClient   *testresources.TestClient
	backupClient *backuplib.Client
}

func (suite *BackupRestoreTestSuite) SetupSuite() {
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

func (suite *BackupRestoreTestSuite) TearDownSuite() {
	suite.Aeroclient.Close()
}

func (suite *BackupRestoreTestSuite) SetupTest() {
	suite.testClient.Truncate(namespace, "")
}

func (suite *BackupRestoreTestSuite) TearDownTest() {
	suite.testClient.Truncate(namespace, "")
}

// TODO make sure that sindex NULL values are filtered out
func (suite *BackupRestoreTestSuite) TestBackupToWriter() {
	// Write some records to the database
	expectedRecs := make([]*a.Record, 10)
	for i := 0; i < 10; i++ {
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

	err := suite.testClient.WriteRecords(10, namespace, "", expectedRecs)
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

	fmt.Print(dst.String())

	suite.testClient.Truncate(namespace, "")

	// TODO restore the records and check that they are the same

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

	err = suite.testClient.ValidateRecords(expectedRecs, 10, namespace, "")
	suite.Assert().Nil(err)

}

func TestBackupRestoreTestSuite(t *testing.T) {
	suite.Run(t, new(BackupRestoreTestSuite))
}
