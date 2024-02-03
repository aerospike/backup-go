package backuplib_test

import (
	"backuplib"
	"backuplib/decoder"
	testresources "backuplib/test_resources"
	"math/rand"
	"path/filepath"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	Host          = "127.0.0.1"
	Port          = 3000
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

func (suite *BackupRestoreTestSuite) TestFile() {

	filePath := filepath.Join(BackupDirPath, "testfile")

	bargs := backuplib.BackupFileArgs{
		Namespace:  "test",
		Marshaller: nil,
		Parallel:   1,
		FilePath:   filePath,
	}

	bhandler, err := suite.backupClient.BackupFile(bargs)
	assert.NoError(suite.T(), err)

	bhandler.Wait()

	rargs := backuplib.RestoreFileArgs{
		NewDecoder: decoder.NewASBReader(),
		FilePath:   filePath,
		Parallel:   1,
	}
}
