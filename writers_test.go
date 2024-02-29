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

package backuplib

import (
	"bytes"
	"context"
	"errors"
	"testing"

	enc_mocks "github.com/aerospike/aerospike-tools-backup-lib/encoding/mocks"
	"github.com/aerospike/aerospike-tools-backup-lib/mocks"
	"github.com/aerospike/aerospike-tools-backup-lib/models"

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

	expRecord := &models.Record{
		Key: key,
		Bins: a.BinMap{
			"key0": "hi",
			"key1": 1,
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
	mockEncoder.EXPECT().EncodeToken(recToken).Return([]byte("rec,"), nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return([]byte("si,"), nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return([]byte("udf"), nil)
	mockEncoder.EXPECT().EncodeToken(invalidToken).Return(nil, errors.New("error"))

	output := &bytes.Buffer{}

	writer := newGenericWriter(mockEncoder, output)
	suite.NotNil(writer)

	err := writer.Write(recToken)
	suite.Nil(err)
	suite.Equal("rec,", output.String())

	err = writer.Write(SIndexToken)
	suite.Nil(err)
	suite.Equal("rec,si,", output.String())

	err = writer.Write(UDFToken)
	suite.Nil(err)
	suite.Equal("rec,si,udf", output.String())

	err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	suite.NotNil(err)
	suite.Equal("rec,si,udf", output.String())

	writer.Close()

	mockEncoder.AssertExpectations(suite.T())

	// Encoder failed

	failRec := &models.Record{}
	failRecToken := models.NewRecordToken(failRec)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(nil, errors.New("error"))
	err = writer.Write(failRecToken)
	suite.NotNil(err)
	suite.Equal("rec,si,udf", output.String())

	mockEncoder.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestASBWriter() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	expRecord := &models.Record{
		Key: key,
		Bins: a.BinMap{
			"key0": "hi",
			"key1": 1,
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
	mockEncoder.EXPECT().GetVersionText().Return([]byte("Version 3.1\n"))
	mockEncoder.EXPECT().GetNamespaceMetaText(namespace).Return([]byte("# namespace test\n"))
	mockEncoder.EXPECT().GetFirstMetaText().Return([]byte("# first-file\n"))
	mockEncoder.EXPECT().EncodeToken(recToken).Return([]byte("rec,"), nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return([]byte("si,"), nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return([]byte("udf"), nil)

	output := &bytes.Buffer{}

	writer := newAsbWriter(mockEncoder, output)
	suite.NotNil(writer)

	err := writer.Init(namespace, true)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\n", output.String())

	err = writer.Write(recToken)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,", output.String())

	err = writer.Write(SIndexToken)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,si,", output.String())

	err = writer.Write(UDFToken)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,si,udf", output.String())

	writer.Close()

	mockEncoder.AssertExpectations(suite.T())

	// Encoder failed

	failRec := &models.Record{}
	failRecToken := models.NewRecordToken(failRec)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(nil, errors.New("error"))
	err = writer.Write(failRecToken)
	suite.NotNil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,si,udf", output.String())

	mockEncoder.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestRestoreWriter() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	expRecord := &models.Record{
		Key: key,
		Bins: a.BinMap{
			"key0": "hi",
			"key1": 1,
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

	failRec := &models.Record{}
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

	expRecord := &models.Record{
		Key: key,
		Bins: a.BinMap{
			"key0": "hi",
			"key1": 1,
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
