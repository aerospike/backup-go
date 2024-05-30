package aerospike

import (
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}

func (suite *writersTestSuite) TestRestoreWriterRecord() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	expRecord := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	recToken := models.NewRecordToken(expRecord, 0)

	policy := &a.WritePolicy{}
	mockDBWriter := mocks.NewDbWriter(suite.T())
	mockDBWriter.EXPECT().Put(policy, expRecord.Key, expRecord.Bins).Return(nil)

	writer := NewRestoreWriter(mockDBWriter, policy, &models.RestoreStats{}, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(recToken)
	suite.Nil(err)

	writer.Close()

	mockDBWriter.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestRestoreWriterRecordFail() {
	namespace := "test"
	set := ""
	key, _ := a.NewKey(namespace, set, "key")
	mockDBWriter := mocks.NewDbWriter(suite.T())
	policy := &a.WritePolicy{}
	writer := NewRestoreWriter(mockDBWriter, policy, &models.RestoreStats{}, slog.Default())
	rec := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	failRecToken := models.NewRecordToken(rec, 0)
	mockDBWriter.EXPECT().Put(policy, rec.Key, rec.Bins).Return(a.ErrInvalidParam)
	_, err := writer.Write(failRecToken)
	suite.NotNil(err)

	mockDBWriter.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestRestoreWriterWithPolicy() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	expRecord := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	recToken := models.NewRecordToken(expRecord, 120)

	policy := a.NewWritePolicy(1, 0)

	mockDBWriter := mocks.NewDbWriter(suite.T())
	mockDBWriter.EXPECT().Put(policy, expRecord.Key, expRecord.Bins).Return(nil)

	stats := &models.RestoreStats{}
	writer := NewRestoreWriter(mockDBWriter, policy, stats, slog.Default())
	suite.NotNil(writer)
	n, err := writer.Write(recToken)

	suite.Equal(120, n)
	suite.Equal(1, int(stats.GetRecordsInserted()))
	suite.Nil(err)

	writer.Close()
}
