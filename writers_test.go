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

package backup

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	encmocks "github.com/aerospike/backup-go/encoding/mocks"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
}

func (suite *writersTestSuite) TestWriteWorker() {
	mockWriter := mocks.NewDataWriter[string](suite.T())
	mockWriter.EXPECT().Write("test").Return(1, nil)
	mockWriter.EXPECT().Close()

	worker := newWriteWorker(mockWriter)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)
}

func (suite *writersTestSuite) TestWriteWorkerClose() {
	mockWriter := mocks.NewDataWriter[string](suite.T())
	mockWriter.EXPECT().Close()

	worker := newWriteWorker(mockWriter)
	suite.NotNil(worker)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := worker.Run(ctx)
	suite.NotNil(err)
}

func (suite *writersTestSuite) TestWriteWorkerWriteFailed() {
	mockWriter := mocks.NewDataWriter[string](suite.T())
	mockWriter.EXPECT().Write("test").Return(0, errors.New("error"))
	mockWriter.EXPECT().Close()

	worker := newWriteWorker(mockWriter)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.NotNil(err)
}

func (suite *writersTestSuite) TestTokenWriter() {
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
	recToken := models.NewRecordToken(expRecord)

	expUDF := &models.UDF{
		Name: "udf",
	}
	UDFToken := models.NewUDFToken(expUDF)

	expSIndex := &models.SIndex{
		Name: "sindex",
	}
	SIndexToken := models.NewSIndexToken(expSIndex)

	invalidToken := &models.Token{Type: models.TokenTypeInvalid}

	mockEncoder := encmocks.NewEncoder(suite.T())
	mockEncoder.EXPECT().EncodeToken(recToken).Return([]byte("encoded rec "), nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return([]byte("encoded sindex "), nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return([]byte("encoded udf "), nil)
	mockEncoder.EXPECT().EncodeToken(invalidToken).Return(nil, errors.New("error"))

	dst := bytes.Buffer{}
	writer := newTokenWriter(mockEncoder, &dst, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(recToken)
	suite.Nil(err)
	suite.Equal("encoded rec ", dst.String())

	_, err = writer.Write(SIndexToken)
	suite.Nil(err)
	suite.Equal("encoded rec encoded sindex ", dst.String())

	_, err = writer.Write(UDFToken)
	suite.Nil(err)
	suite.Equal("encoded rec encoded sindex encoded udf ", dst.String())

	_, err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	suite.NotNil(err)
	suite.Equal("encoded rec encoded sindex encoded udf ", dst.String())

	failRec := models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(nil, errors.New("error"))
	_, err = writer.Write(failRecToken)
	suite.NotNil(err)

	writer.Close()
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
	recToken := models.NewRecordToken(expRecord)

	mockDBWriter := mocks.NewDbWriter(suite.T())
	mockDBWriter.EXPECT().Put((*a.WritePolicy)(nil), expRecord.Key, expRecord.Bins).Return(nil)

	writer := newRestoreWriter(mockDBWriter, nil, &RestoreStats{}, slog.Default())
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
	writer := newRestoreWriter(mockDBWriter, nil, &RestoreStats{}, slog.Default())
	rec := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	failRecToken := models.NewRecordToken(rec)
	mockDBWriter.EXPECT().Put((*a.WritePolicy)(nil), rec.Key, rec.Bins).Return(a.ErrInvalidParam)
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
	recToken := models.NewRecordToken(expRecord)

	policy := a.NewWritePolicy(1, 0)

	mockDBWriter := mocks.NewDbWriter(suite.T())
	mockDBWriter.EXPECT().Put(policy, expRecord.Key, expRecord.Bins).Return(nil)

	writer := newRestoreWriter(mockDBWriter, policy, nil, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(recToken)
	suite.Nil(err)

	writer.Close()
}

func (suite *writersTestSuite) TestTokenStatsWriter() {
	mockWriter := mocks.NewDataWriter[*models.Token](suite.T())
	mockWriter.EXPECT().Write(models.NewRecordToken(models.Record{})).Return(1, nil)
	mockWriter.EXPECT().Write(models.NewSIndexToken(&models.SIndex{})).Return(1, nil)
	mockWriter.EXPECT().Write(models.NewUDFToken(&models.UDF{})).Return(1, nil)
	mockWriter.EXPECT().Write(&models.Token{Type: models.TokenTypeInvalid}).Return(0, errors.New("error"))
	mockWriter.EXPECT().Close()

	mockStats := newMockStatsSetterToken(suite.T())
	mockStats.EXPECT().addRecords(uint64(1))
	mockStats.EXPECT().addUDFs(uint32(1))
	mockStats.EXPECT().addSIndexes(uint32(1))
	mockStats.EXPECT().addTotalSize(uint64(1))

	writer := newWriterWithTokenStats(mockWriter, mockStats, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(models.NewRecordToken(models.Record{}))
	suite.Nil(err)

	_, err = writer.Write(models.NewSIndexToken(&models.SIndex{}))
	suite.Nil(err)

	_, err = writer.Write(models.NewUDFToken(&models.UDF{}))
	suite.Nil(err)

	_, err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	suite.NotNil(err)

	writer.Close()
}

func (suite *writersTestSuite) TestTokenStatsWriterWriterFailed() {
	mockWriter := mocks.NewDataWriter[*models.Token](suite.T())
	mockWriter.EXPECT().Write(models.NewSIndexToken(&models.SIndex{})).Return(0, errors.New("error"))

	mockStats := newMockStatsSetterToken(suite.T())

	writer := newWriterWithTokenStats(mockWriter, mockStats, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(models.NewSIndexToken(&models.SIndex{}))
	suite.Error(err)
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}
