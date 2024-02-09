package backuplib_test

import (
	"backuplib"
	testresources "backuplib/test_resources"
	"bytes"
	"fmt"
	"io"
	"math/rand"
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

	randSource := rand.NewSource(0)
	dataGenFactory := testresources.NewASDataGeneratorFactory()
	testClient := testresources.NewTestClient(asc, &randSource, dataGenFactory)
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
	err := suite.testClient.WriteRecords(10, namespace, "")
	if err != nil {
		panic(err)
	}

	dst := bytes.NewBuffer([]byte{})
	enc := backuplib.NewASBEncoderFactory()

	_, errors := suite.backupClient.BackupToWriter(
		[]io.Writer{dst},
		enc,
		"test",
		backuplib.BackupToWriterOptions{
			Parallel: 1,
		},
	)

	err = <-errors
	suite.Assert().Nil(err)

	fmt.Print(dst.String())

	// TODO restore the records and check that they are the same
}

func TestBackupRestoreTestSuite(t *testing.T) {
	suite.Run(t, new(BackupRestoreTestSuite))
}
