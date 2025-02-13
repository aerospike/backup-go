// Copyright 2024 Aerospike, Inc.
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
	"errors"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	pipemocks "github.com/aerospike/backup-go/pipeline/mocks"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
}

func (suite *writersTestSuite) TestTokenWriter() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	expRecord := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	recToken := models.NewRecordToken(expRecord, 0, nil)

	expUDF := &models.UDF{
		Name: "udf",
	}
	UDFToken := models.NewUDFToken(expUDF, 0)

	expSIndex := &models.SIndex{
		Name: "sindex",
	}
	SIndexToken := models.NewSIndexToken(expSIndex, 0)

	invalidToken := &models.Token{Type: models.TokenTypeInvalid}

	mockEncoder := mocks.NewMockEncoder(suite.T())
	mockEncoder.EXPECT().EncodeToken(recToken).Return([]byte("encoded rec "), nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return([]byte("encoded sindex "), nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return([]byte("encoded udf "), nil)
	mockEncoder.EXPECT().EncodeToken(invalidToken).Return(nil, errors.New("error"))

	dst := bytes.Buffer{}
	writer := newTokenWriter(mockEncoder, &dst, slog.Default(), nil)
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

	failRec := &models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec, 0, nil)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(nil, errors.New("error"))
	_, err = writer.Write(failRecToken)
	suite.NotNil(err)

	err = writer.Close()
	suite.Nil(err)
}

func (suite *writersTestSuite) TestTokenStatsWriter() {
	mockWriter := pipemocks.NewMockDataWriter[*models.Token](suite.T())
	mockWriter.EXPECT().Write(models.NewRecordToken(&models.Record{}, 0, nil)).Return(1, nil)
	mockWriter.EXPECT().Write(models.NewSIndexToken(&models.SIndex{}, 0)).Return(1, nil)
	mockWriter.EXPECT().Write(models.NewUDFToken(&models.UDF{}, 0)).Return(1, nil)
	mockWriter.EXPECT().Write(&models.Token{Type: models.TokenTypeInvalid}).Return(0, errors.New("error"))
	mockWriter.EXPECT().Close().Return(nil)

	mockStats := mocks.NewMockstatsSetterToken(suite.T())
	mockStats.EXPECT().AddUDFs(uint32(1))
	mockStats.EXPECT().AddSIndexes(uint32(1))

	writer := newWriterWithTokenStats(mockWriter, mockStats, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(models.NewRecordToken(&models.Record{}, 0, nil))
	suite.Nil(err)

	_, err = writer.Write(models.NewSIndexToken(&models.SIndex{}, 0))
	suite.Nil(err)

	_, err = writer.Write(models.NewUDFToken(&models.UDF{}, 0))
	suite.Nil(err)

	_, err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	suite.NotNil(err)

	err = writer.Close()
	suite.Nil(err)
}

func (suite *writersTestSuite) TestTokenStatsWriterWriterFailed() {
	mockWriter := pipemocks.NewMockDataWriter[*models.Token](suite.T())
	mockWriter.EXPECT().Write(models.NewSIndexToken(&models.SIndex{}, 0)).Return(0, errors.New("error"))

	mockStats := mocks.NewMockstatsSetterToken(suite.T())

	writer := newWriterWithTokenStats(mockWriter, mockStats, slog.Default())
	suite.NotNil(writer)

	_, err := writer.Write(models.NewSIndexToken(&models.SIndex{}, 0))
	suite.Error(err)
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}
