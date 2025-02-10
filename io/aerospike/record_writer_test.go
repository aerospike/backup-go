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

package aerospike

import (
	"log/slog"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/mock"
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

	policy := &a.WritePolicy{}
	mockDBWriter := mocks.NewMockdbWriter(suite.T())
	mockDBWriter.EXPECT().Put(policy, expRecord.Key, expRecord.Bins).Return(nil)

	stats := models.NewRestoreStats()
	writer := newRecordWriter(mockDBWriter, policy, stats, slog.Default(), false, 1, nil, false)
	suite.NotNil(writer)

	err := writer.writeRecord(&expRecord)
	suite.Nil(err)
	suite.Equal(1, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestRestoreWriterRecordFail() {
	namespace := "test"
	set := ""
	key, _ := a.NewKey(namespace, set, "key")
	mockDBWriter := mocks.NewMockdbWriter(suite.T())
	policy := &a.WritePolicy{}
	stats := models.NewRestoreStats()
	writer := newRecordWriter(mockDBWriter, policy, stats, slog.Default(), false, 1, nil, false)
	rec := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	mockDBWriter.EXPECT().Put(policy, rec.Key, rec.Bins).Return(a.ErrInvalidParam)
	err := writer.writeRecord(&rec)
	suite.NotNil(err)
	suite.Equal(0, int(stats.GetRecordsInserted()))

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

	policy := a.NewWritePolicy(1, 0)

	mockDBWriter := mocks.NewMockdbWriter(suite.T())
	mockDBWriter.EXPECT().Put(policy, expRecord.Key, expRecord.Bins).Return(nil)

	stats := models.NewRestoreStats()
	writer := newRecordWriter(mockDBWriter, policy, stats, slog.Default(), false, 1, nil, false)
	suite.NotNil(writer)

	err := writer.writeRecord(&expRecord)

	suite.Nil(err)
	suite.Equal(1, int(stats.GetRecordsInserted()))
}

func (suite *writersTestSuite) TestSingleRecordWriterRetry() {
	namespace := "test"
	set := ""
	key, _ := a.NewKey(namespace, set, "key")
	mockDBWriter := mocks.NewMockdbWriter(suite.T())
	policy := &a.WritePolicy{}
	stats := models.NewRestoreStats()
	retryPolicy := &models.RetryPolicy{
		BaseTimeout: 10 * time.Millisecond,
		Multiplier:  1,
		MaxRetries:  2,
	}
	writer := newRecordWriter(mockDBWriter, policy, stats, slog.Default(), false, 1, retryPolicy, false)
	rec := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}

	mockDBWriter.On("Put", policy, rec.Key, rec.Bins).
		Return(a.ErrConnectionPoolEmpty).Once()
	mockDBWriter.On("Put", policy, rec.Key, rec.Bins).
		Return(a.ErrTimeout).Once()
	mockDBWriter.On("Put", policy, rec.Key, rec.Bins).
		Return(nil).Once()

	err := writer.writeRecord(&rec)
	suite.Nil(err)

	err = writer.close()
	suite.Nil(err)
	suite.Equal(1, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(suite.T())
}

func (suite *writersTestSuite) TestBatchRecordWriterRetry() {
	namespace := "test"
	set := ""
	key, _ := a.NewKey(namespace, set, "key")
	mockDBWriter := mocks.NewMockdbWriter(suite.T())
	policy := &a.WritePolicy{}
	stats := models.NewRestoreStats()
	retryPolicy := &models.RetryPolicy{
		BaseTimeout: 10 * time.Millisecond,
		Multiplier:  1,
		MaxRetries:  2,
	}
	writer := newRecordWriter(mockDBWriter, policy, stats, slog.Default(), true, 1, retryPolicy, false)
	rec := models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}

	mockDBWriter.On("BatchOperate", mock.Anything, mock.Anything).
		Return(a.ErrConnectionPoolEmpty).Once()
	mockDBWriter.On("BatchOperate", mock.Anything, mock.Anything).
		Return(a.ErrTimeout).Once()
	mockDBWriter.On("BatchOperate", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			batchOps := args.Get(1).([]a.BatchRecordIfc)
			for _, op := range batchOps {
				op.BatchRec().ResultCode = types.OK
			}
		}).Return(nil).Once()

	err := writer.writeRecord(&rec)
	suite.Nil(err)

	err = writer.close()
	suite.Nil(err)
	suite.Equal(1, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(suite.T())
}
