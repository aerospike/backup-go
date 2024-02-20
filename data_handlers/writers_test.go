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

package datahandlers

import (
	"bytes"
	"errors"
	"testing"

	"backuplib/data_handlers/mocks"
	"backuplib/models"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
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

	expUDF := &models.UDF{
		Name: "udf",
	}

	expSIndex := &models.SIndex{
		Name: "sindex",
	}

	mockEncoder := mocks.NewEncoder(suite.T())
	mockEncoder.EXPECT().EncodeRecord(expRecord).Return([]byte("rec,"), nil)
	mockEncoder.EXPECT().EncodeSIndex(expSIndex).Return([]byte("si,"), nil)
	mockEncoder.EXPECT().EncodeUDF(expUDF).Return([]byte("udf"), nil)

	output := &bytes.Buffer{}

	writer := NewGenericWriter(mockEncoder, output)
	suite.NotNil(writer)

	err := writer.Write(expRecord)
	suite.Nil(err)
	suite.Equal("rec,", output.String())

	err = writer.Write(expSIndex)
	suite.Nil(err)
	suite.Equal("rec,si,", output.String())

	err = writer.Write(expUDF)
	suite.Nil(err)
	suite.Equal("rec,si,udf", output.String())

	err = writer.Write("bad_type")
	suite.NotNil(err)
	suite.Equal("rec,si,udf", output.String())

	writer.Cancel()

	mockEncoder.AssertExpectations(suite.T())

	// Encoder failed

	failRec := &models.Record{}
	mockEncoder.EXPECT().EncodeRecord(failRec).Return(nil, errors.New("error"))
	err = writer.Write(failRec)
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

	expUDF := &models.UDF{
		Name: "udf",
	}

	expSIndex := &models.SIndex{
		Name: "sindex",
	}

	mockEncoder := mocks.NewASBEncoder(suite.T())
	mockEncoder.EXPECT().GetVersionText().Return([]byte("Version 3.1\n"))
	mockEncoder.EXPECT().GetNamespaceMetaText(namespace).Return([]byte("# namespace test\n"))
	mockEncoder.EXPECT().GetFirstMetaText().Return([]byte("# first-file\n"))
	mockEncoder.EXPECT().EncodeRecord(expRecord).Return([]byte("rec,"), nil)
	mockEncoder.EXPECT().EncodeSIndex(expSIndex).Return([]byte("si,"), nil)
	mockEncoder.EXPECT().EncodeUDF(expUDF).Return([]byte("udf"), nil)

	output := &bytes.Buffer{}

	writer := NewASBWriter(mockEncoder, output)
	suite.NotNil(writer)

	err := writer.Init(namespace, true)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\n", output.String())

	err = writer.Write(expRecord)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,", output.String())

	err = writer.Write(expSIndex)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,si,", output.String())

	err = writer.Write(expUDF)
	suite.Nil(err)
	suite.Equal("Version 3.1\n# namespace test\n# first-file\nrec,si,udf", output.String())

	writer.Cancel()

	mockEncoder.AssertExpectations(suite.T())

	// Encoder failed

	failRec := &models.Record{}
	mockEncoder.EXPECT().EncodeRecord(failRec).Return(nil, errors.New("error"))
	err = writer.Write(failRec)
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

	mockDBWriter := mocks.NewDBWriter(suite.T())
	mockDBWriter.EXPECT().Put((*a.WritePolicy)(nil), expRecord.Key, expRecord.Bins).Return(nil)

	writer := NewRestoreWriter(mockDBWriter)
	suite.NotNil(writer)

	err := writer.Write(expRecord)
	suite.Nil(err)

	writer.Cancel()

	mockDBWriter.AssertExpectations(suite.T())

	// DBWriter failed

	failRec := &models.Record{}
	mockDBWriter.EXPECT().Put((*a.WritePolicy)(nil), failRec.Key, failRec.Bins).Return(a.ErrInvalidParam)
	err = writer.Write(failRec)
	suite.NotNil(err)

	mockDBWriter.AssertExpectations(suite.T())
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}
