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
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	enc_mocks "github.com/aerospike/backup-go/encoding/mocks"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

type readersTestSuite struct {
	suite.Suite
}

func (suite *readersTestSuite) TestReadWorker() {
	mockReader := mocks.NewDataReader[string](suite.T())

	readCalls := 0
	mockReader.EXPECT().Read().RunAndReturn(func() (string, error) {
		readCalls++
		if readCalls <= 3 {
			return "hi", nil
		}
		return "", io.EOF
	})
	mockReader.EXPECT().Close()

	worker := newReadWorker[string](mockReader)
	suite.NotNil(worker)

	send := make(chan string, 3)
	worker.SetSendChan(send)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)
	close(send)

	suite.Equal(3, len(send))

	for v := range send {
		suite.Equal("hi", v)
	}
}

func (suite *readersTestSuite) TestReadWorkerClose() {
	mockReader := mocks.NewDataReader[string](suite.T())
	mockReader.EXPECT().Read().Return("hi", nil)
	mockReader.EXPECT().Close()

	worker := newReadWorker[string](mockReader)
	suite.NotNil(worker)

	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		suite.NotNil(err)
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()
}

func (suite *readersTestSuite) TestGenericReader() {
	key, aerr := a.NewKey("test", "", "key")
	if aerr != nil {
		panic(aerr)
	}

	mockRec := models.Record{
		Record: &a.Record{
			Bins: a.BinMap{
				"key": "hi",
			},
			Key: key,
		},
	}
	expectedRecToken := models.NewRecordToken(mockRec)

	mockDecoder := enc_mocks.NewDecoder(suite.T())
	mockDecoder.EXPECT().NextToken().Return(expectedRecToken, nil)

	reader := newGenericReader(mockDecoder)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	suite.Equal(expectedRecToken, v)

	reader.Close()

	mockDecoder.AssertExpectations(suite.T())
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

	mockScanner := mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		(*a.ScanPolicy)(nil),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := newAerospikeRecordReader(
		mockScanner,
		arrConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		nil,
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	expectedRecToken := models.NewRecordToken(mockRec)
	suite.Equal(expectedRecToken, v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestAerospikeRecordReaderNotStarted() {
	reader := &aerospikeRecordReader{
		status: arrStatus{
			started: false,
		},
	}

	reader.Close()
	suite.False(reader.status.started)
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

	mockScanner := mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		(*a.ScanPolicy)(nil),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := newAerospikeRecordReader(
		mockScanner,
		arrConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		nil,
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

	mockScanner := mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		(*a.ScanPolicy)(nil),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := newAerospikeRecordReader(
		mockScanner,
		arrConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		nil,
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

	mockScanner := mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		(*a.ScanPolicy)(nil),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		nil,
		a.ErrInvalidParam,
	)

	reader := newAerospikeRecordReader(
		mockScanner,
		arrConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		nil,
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

	mockScanner := mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		policy,
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := newAerospikeRecordReader(
		mockScanner,
		arrConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		policy,
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	expectedRecToken := models.NewRecordToken(mockRec)
	suite.Equal(expectedRecToken, v)
	mockScanner.AssertExpectations(suite.T())
}

func (suite *readersTestSuite) TestSIndexReader() {
	namespace := "test"
	mockSIndexGetter := mocks.NewSindexGetter(suite.T())
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

	reader := newSIndexReader(mockSIndexGetter, namespace)
	suite.NotNil(reader)

	expectedSIndexTokens := []*models.Token{}
	for _, sindex := range mockSIndexes {
		expectedSIndexTokens = append(expectedSIndexTokens, models.NewSIndexToken(sindex))
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

	mockSIndexGetter = mocks.NewSindexGetter(suite.T())
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		nil,
		fmt.Errorf("error"),
	)

	reader = newSIndexReader(mockSIndexGetter, namespace)
	suite.NotNil(reader)

	v, err = reader.Read()
	suite.NotNil(err)
	suite.Nil(v)

	mockSIndexGetter.AssertExpectations(suite.T())
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
