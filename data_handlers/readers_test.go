package datahandlers

import (
	"fmt"
	"io"
	"reflect"
	"testing"
	"unsafe"

	"backuplib/data_handlers/mocks"
	"backuplib/models"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

type readersTestSuite struct {
	suite.Suite
}

func (suite *readersTestSuite) TestGenericReader() {
	mockDecoder := mocks.NewDecoder(suite.T())
	mockDecoder.EXPECT().NextToken().Return("hi", nil)

	reader := NewGenericReader(mockDecoder)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	suite.Equal("hi", v)

	reader.Cancel()

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
	mockRec := &a.Record{
		Bins: a.BinMap{
			"key": "hi",
		},
		Key: key,
	}
	mockRes := &a.Result{
		Record: mockRec,
	}
	mockResults <- mockRes
	setFieldValue(mockRecordSet, "records", mockResults)

	mockScanner := mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		a.NewScanPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader := NewAerospikeRecordReader(
		&ARRConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		mockScanner,
	)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	suite.Equal(mockRec, v)
	mockScanner.AssertExpectations(suite.T())

	// positive channel closed
	close(mockResults)

	mockScanner = mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		a.NewScanPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader = NewAerospikeRecordReader(
		&ARRConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		mockScanner,
	)
	suite.NotNil(reader)

	v, err = reader.Read()
	suite.Equal(err, io.EOF)
	suite.Nil(v)
	mockScanner.AssertExpectations(suite.T())

	// negative startScan fails

	mockScanner = mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		a.NewScanPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		nil,
		a.ErrInvalidParam,
	)

	reader = NewAerospikeRecordReader(
		&ARRConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		mockScanner,
	)
	suite.NotNil(reader)

	v, err = reader.Read()
	suite.NotNil(err)
	suite.Nil(v)
	mockScanner.AssertExpectations(suite.T())

	// negative record res error

	mockRecordSet = &a.Recordset{}
	mockResults = make(chan *a.Result, 1)
	mockRec = &a.Record{
		Bins: a.BinMap{
			"key": "hi",
		},
		Key: key,
	}
	mockRes = &a.Result{
		Record: mockRec,
		Err:    a.ErrInvalidParam,
	}
	mockResults <- mockRes
	setFieldValue(mockRecordSet, "records", mockResults)

	mockScanner = mocks.NewScanner(suite.T())
	mockScanner.EXPECT().ScanPartitions(
		a.NewScanPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	reader = NewAerospikeRecordReader(
		&ARRConfig{
			Namespace:      namespace,
			Set:            set,
			FirstPartition: 0,
			NumPartitions:  4096,
		},
		mockScanner,
	)
	suite.NotNil(reader)

	v, err = reader.Read()
	suite.NotNil(err)
	suite.Nil(v)
	mockScanner.AssertExpectations(suite.T())

	// test cancel not started

	reader = &AerospikeRecordReader{
		status: &ARRStatus{
			started: false,
		},
	}

	reader.Cancel()
	suite.False(reader.status.started)

}

func (suite *readersTestSuite) TestSIndexReader() {

	namespace := "test"

	mockSIndexGetter := mocks.NewSIndexGetter(suite.T())
	expectedSIndexes := []*models.SIndex{
		{
			Namespace: namespace,
			Set:       "set",
		},
		{},
	}
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		expectedSIndexes,
		nil,
	)

	reader := NewSIndexReader(mockSIndexGetter, namespace)
	suite.NotNil(reader)

	v, err := reader.Read()
	suite.Nil(err)
	suite.Equal(v, expectedSIndexes[0])

	v, err = reader.Read()
	suite.Nil(err)
	suite.Equal(v, expectedSIndexes[1])

	v, err = reader.Read()
	suite.Equal(err, io.EOF)
	suite.Nil(v)

	reader.Cancel()

	mockSIndexGetter.AssertExpectations(suite.T())

	// negative GetSindexes fails

	mockSIndexGetter = mocks.NewSIndexGetter(suite.T())
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		nil,
		fmt.Errorf("error"),
	)

	reader = NewSIndexReader(mockSIndexGetter, namespace)
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
