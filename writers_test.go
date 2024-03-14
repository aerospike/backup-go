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
	"context"
	"errors"
	"testing"

	enc_mocks "github.com/aerospike/backup-go/encoding/mocks"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
}

func (suite *writersTestSuite) TestWriteWorker() {
	mockWriter := mocks.NewDataWriter[string](suite.T())
	mockWriter.EXPECT().Write("test").Return(nil)
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
	mockWriter.EXPECT().Write("test").Return(errors.New("error"))
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

func (suite *writersTestSuite) TestGenericWriter() {
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

	mockEncoder := enc_mocks.NewEncoder(suite.T())
	mockEncoder.EXPECT().EncodeToken(recToken).Return(1, nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return(2, nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return(3, nil)
	mockEncoder.EXPECT().EncodeToken(invalidToken).Return(0, errors.New("error"))

	writer := newGenericWriter(mockEncoder)
	suite.NotNil(writer)

	err := writer.Write(recToken)
	suite.Nil(err)

	err = writer.Write(SIndexToken)
	suite.Nil(err)

	err = writer.Write(UDFToken)
	suite.Nil(err)

	err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	suite.NotNil(err)

	writer.Close()

	mockEncoder.AssertExpectations(suite.T())

	// Encoder failed

	failRec := models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(0, errors.New("error"))
	err = writer.Write(failRecToken)
	suite.NotNil(err)

	mockEncoder.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestASBWriter() {
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

	mockEncoder := mocks.NewAsbEncoder(suite.T())
	mockEncoder.EXPECT().WriteHeader(namespace, true).Return(1, nil)
	mockEncoder.EXPECT().EncodeToken(recToken).Return(2, nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return(3, nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return(4, nil)

	writer := newAsbWriter(mockEncoder)
	suite.NotNil(writer)

	err := writer.Init(namespace, true)
	suite.Nil(err)

	err = writer.Write(recToken)
	suite.Nil(err)

	err = writer.Write(SIndexToken)
	suite.Nil(err)

	err = writer.Write(UDFToken)
	suite.Nil(err)

	writer.Close()

	mockEncoder.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestASBWriterNegative() {
	mockEncoder := mocks.NewAsbEncoder(suite.T())

	writer := newAsbWriter(mockEncoder)
	suite.NotNil(writer)

	failRec := models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(0, errors.New("error"))
	err := writer.Write(failRecToken)
	suite.NotNil(err)

	mockEncoder.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestRestoreWriter() {
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

	writer := newRestoreWriter(mockDBWriter, nil)
	suite.NotNil(writer)

	err := writer.Write(recToken)
	suite.Nil(err)

	writer.Close()

	mockDBWriter.AssertExpectations(suite.T())

	// DBWriter failed

	failRec := models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec)
	mockDBWriter.EXPECT().Put((*a.WritePolicy)(nil), failRec.Key, failRec.Bins).Return(a.ErrInvalidParam)
	err = writer.Write(failRecToken)
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

	writer := newRestoreWriter(mockDBWriter, policy)
	suite.NotNil(writer)

	err := writer.Write(recToken)
	suite.Nil(err)

	writer.Close()
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}
