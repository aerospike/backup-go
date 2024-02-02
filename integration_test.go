package backuplib_test

import (
	testresources "backuplib/test_resources"
	"math/rand"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

const (
	Host = "127.0.0.1"
	Port = 3000
)

type BackupRestoreTestSuite struct {
	suite.Suite
	client     *a.Client
	testClient *testresources.TestClient
}

func (suite *BackupRestoreTestSuite) SetupSuite() {
	asc, err := a.NewClientWithPolicy(
		a.NewClientPolicy(),
		Host,
		Port,
	)

	if err != nil {
		panic(err)
	}

	suite.client = asc

	randSource := rand.NewSource(0)
	dataGenFactory := testresources.NewASDataGeneratorFactory()
	testClient := testresources.NewTestClient(asc, &randSource, dataGenFactory)
	suite.testClient = testClient
}

func (suite *BackupRestoreTestSuite) TearDownSuite() {
	suite.client.Close()
}

func (suite *BackupRestoreTestSuite) TestBackupRestoreFile() {

}
