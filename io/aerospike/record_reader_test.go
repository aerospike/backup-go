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

//go:build test
// +build test

package aerospike

import (
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"unsafe"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/suite"
)

type readersTestSuite struct {
	suite.Suite
}

func (suite *readersTestSuite) TestAerospikeRecordReader() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	rec := &a.Record{
		Bins: a.BinMap{
			"key": "hi",
		},
		Key: key,
	}
	mockRes := &a.Result{
		Record: rec,
	}
	mockRec := models.Record{
		Record: rec,
	}
	mockResults <- mockRes
	setFieldValue(mockRecordSet, "records", mockResults)

	mockScanner := mocks.NewMockscanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		&a.ScanPolicy{},
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := NewRecordReader(
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
		},
		slog.Default(),
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	expectedRecToken := models.NewRecordToken(mockRec, 0)
	suite.Equal(expectedRecToken, v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestAerospikeRecordReaderRecordResError() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	mockRec := &a.Record{
		Bins: a.BinMap{
			"key": "hi",
		},
		Key: key,
	}
	mockRes := &a.Result{
		Record: mockRec,
		Err:    a.ErrInvalidParam,
	}
	mockResults <- mockRes
	setFieldValue(mockRecordSet, "records", mockResults)

	mockScanner := mocks.NewMockscanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		&a.ScanPolicy{},
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := NewRecordReader(
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
		},
		slog.Default(),
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.NotNil(err)
	suite.Nil(v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestAerospikeRecordReaderClosedChannel() {
	namespace := "test"
	set := ""

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	setFieldValue(mockRecordSet, "records", mockResults)

	close(mockResults)

	mockScanner := mocks.NewMockscanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		&a.ScanPolicy{},
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := NewRecordReader(
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
		},
		slog.Default(),
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Equal(io.EOF, err)
	suite.Nil(v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestAerospikeRecordReaderReadFailed() {
	namespace := "test"
	set := ""

	mockScanner := mocks.NewMockscanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		&a.ScanPolicy{},
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		nil,
		a.ErrInvalidParam,
	)

	reader := NewRecordReader(
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
		},
		slog.Default(),
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.NotNil(err)
	suite.Nil(v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestAerospikeRecordReaderWithPolicy() {
	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	rec := &a.Record{
		Bins: a.BinMap{
			"key": "hi",
		},
		Key: key,
	}
	mockRes := &a.Result{
		Record: rec,
	}
	mockRec := models.Record{
		Record: rec,
	}
	mockResults <- mockRes
	setFieldValue(mockRecordSet, "records", mockResults)

	policy := a.NewScanPolicy()
	policy.MaxRecords = 10

	mockScanner := mocks.NewMockscanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		policy,
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := NewRecordReader(
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      policy,
		},
		slog.Default(),
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	expectedRecToken := models.NewRecordToken(mockRec, 0)
	suite.Equal(expectedRecToken, v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestSIndexReader() {
	namespace := "test"
	mockSIndexGetter := mocks.NewMocksindexGetter(suite.T())
	mockSIndexes := []*models.SIndex{
		{
			Namespace: namespace,
			Set:       "set",
		},
		{},
	}
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		mockSIndexes,
		nil,
	)

	reader := NewSIndexReader(mockSIndexGetter, namespace, slog.Default())
	suite.NotNil(reader)

	expectedSIndexTokens := make([]*models.Token, 0, len(mockSIndexes))
	for _, sindex := range mockSIndexes {
		expectedSIndexTokens = append(expectedSIndexTokens, models.NewSIndexToken(sindex, 0))
	}

	v, err := reader.Read()
	suite.Nil(err)
	suite.Equal(v, expectedSIndexTokens[0])

	v, err = reader.Read()
	suite.Nil(err)
	suite.Equal(v, expectedSIndexTokens[1])

	v, err = reader.Read()
	suite.Equal(err, io.EOF)
	suite.Nil(v)

	reader.Close()

	mockSIndexGetter.AssertExpectations(suite.T())

	// negative GetSindexes fails

	mockSIndexGetter = mocks.NewMocksindexGetter(suite.T())
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		nil,
		fmt.Errorf("error"),
	)

	reader = NewSIndexReader(mockSIndexGetter, namespace, slog.Default())
	suite.NotNil(reader)

	v, err = reader.Read()
	suite.NotNil(err)
	suite.Nil(v)

	mockSIndexGetter.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestUDFReader() {
	mockUDFGetter := mocks.NewMockudfGetter(suite.T())
	mockUDFs := []*models.UDF{
		{
			Name: "udf1",
		},
		{
			Name: "udf2",
		},
	}
	mockUDFGetter.EXPECT().GetUDFs().Return(
		mockUDFs,
		nil,
	)

	reader := NewUDFReader(mockUDFGetter, slog.Default())
	suite.NotNil(reader)

	expectedUDFTokens := make([]*models.Token, 0, len(mockUDFs))
	for _, udf := range mockUDFs {
		expectedUDFTokens = append(expectedUDFTokens, models.NewUDFToken(udf, 0))
	}

	v, err := reader.Read()
	suite.Nil(err)
	suite.Equal(v, expectedUDFTokens[0])

	v, err = reader.Read()
	suite.Nil(err)
	suite.Equal(v, expectedUDFTokens[1])

	v, err = reader.Read()
	suite.Equal(err, io.EOF)
	suite.Nil(v)

	reader.Close()
}

func (suite *readersTestSuite) TestUDFReaderReadFailed() {
	mockUDFGetter := mocks.NewMockudfGetter(suite.T())
	mockUDFGetter.EXPECT().GetUDFs().Return(
		nil,
		fmt.Errorf("error"),
	)

	reader := NewUDFReader(mockUDFGetter, slog.Default())
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.NotNil(err)
	suite.Nil(v)
}

func TestReaders(t *testing.T) {
	suite.Run(t, new(readersTestSuite))
}

// setFieldValue is a hack to set the value of an unexported struct field
// it's useful to mock fields in the aerospike go client
// using this lets us avoid mocking the entire client
//
//nolint:unparam // don't complain about the fieldname param always getting "records" it could be any field
func setFieldValue(target any, fieldName string, value any) {
	rv := reflect.ValueOf(target)
	for rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = rv.Elem()
	}
	if !rv.CanAddr() {
		panic("target must be addressable")
	}
	if rv.Kind() != reflect.Struct {
		panic(fmt.Sprintf(
			"unable to set the '%s' field value of the type %T, target must be a struct",
			fieldName,
			target,
		))
	}
	rf := rv.FieldByName(fieldName)

	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
