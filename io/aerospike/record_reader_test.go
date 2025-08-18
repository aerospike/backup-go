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
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"
	"unsafe"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const testMetricMessage = "test metric message"

func TestAerospikeRecordReader(t *testing.T) {
	t.Parallel()

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
	mockRec := &models.Record{
		Record: rec,
	}
	mockResults <- mockRes
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		newExpectedPolicy(), // Use the policy with the expected filter expression
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	ctx := context.Background()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)

	require.Nil(t, err)
	expectedRecToken := models.NewRecordToken(mockRec, 0, nil)
	require.Equal(t, expectedRecToken, v)
	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderRecordResError(t *testing.T) {
	t.Parallel()

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
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		newExpectedPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	ctx := context.Background()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.NotNil(t, err)
	require.Nil(t, v)

	time.Sleep(10 * time.Millisecond)
	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderClosedChannel(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	setFieldValue(mockRecordSet, "records", mockResults)

	close(mockResults)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		newExpectedPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)
	ctx := context.Background()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, v)
	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderReadFailed(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		newExpectedPolicy(),
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		nil,
		a.ErrInvalidParam,
	)

	ctx := context.Background()

	closer := mocks.NewMockRecordsetCloser(t)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.NotNil(t, err)
	require.Nil(t, v)
	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderWithPolicy(t *testing.T) {
	t.Parallel()

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
	mockRec := &models.Record{
		Record: rec,
	}
	mockResults <- mockRes
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	policy := a.NewScanPolicy()
	policy.MaxRecords = 10

	expectedPolicy := a.NewScanPolicy()
	expectedPolicy.MaxRecords = 10
	expectedPolicy.FilterExpression = noMrtSetExpression()

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterByRange(0, 4096),
		namespace,
		set,
	).Return(
		mockRecordSet,
		nil,
	)

	ctx := context.Background()
	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      policy,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.Nil(t, err)
	expectedRecToken := models.NewRecordToken(mockRec, 0, nil)
	require.Equal(t, expectedRecToken, v)

	v, err = reader.Read(ctx)
	require.Equal(t, err, io.EOF)
	require.Nil(t, v)

	mockScanner.AssertExpectations(t)
}
func TestSIndexReader(t *testing.T) {
	t.Parallel()

	namespace := "test"
	mockSIndexGetter := mocks.NewMocksindexGetter(t)
	mockSIndexes := []*models.SIndex{
		{
			Namespace: namespace,
			Set:       "testSet",
		},
		{},
	}
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		mockSIndexes,
		nil,
	)

	reader := NewSIndexReader(mockSIndexGetter, namespace, slog.Default())
	require.NotNil(t, reader)

	expectedSIndexTokens := make([]*models.Token, 0, len(mockSIndexes))
	for _, sindex := range mockSIndexes {
		expectedSIndexTokens = append(expectedSIndexTokens, models.NewSIndexToken(sindex, 0))
	}

	ctx := context.Background()

	v, err := reader.Read(ctx)
	require.Nil(t, err)
	require.Equal(t, v, expectedSIndexTokens[0])

	v, err = reader.Read(ctx)
	require.Nil(t, err)
	require.Equal(t, v, expectedSIndexTokens[1])

	v, err = reader.Read(ctx)
	require.Equal(t, err, io.EOF)
	require.Nil(t, v)

	reader.Close()

	mockSIndexGetter.AssertExpectations(t)

	// negative GetSindexes fails

	mockSIndexGetter = mocks.NewMocksindexGetter(t)
	mockSIndexGetter.EXPECT().GetSIndexes(namespace).Return(
		nil,
		fmt.Errorf("error"),
	)

	reader = NewSIndexReader(mockSIndexGetter, namespace, slog.Default())
	require.NotNil(t, reader)

	v, err = reader.Read(ctx)
	require.NotNil(t, err)
	require.Nil(t, v)

	mockSIndexGetter.AssertExpectations(t)
}

func TestUDFReader(t *testing.T) {
	t.Parallel()

	mockUDFGetter := mocks.NewMockudfGetter(t)
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
	require.NotNil(t, reader)

	expectedUDFTokens := make([]*models.Token, 0, len(mockUDFs))
	for _, udf := range mockUDFs {
		expectedUDFTokens = append(expectedUDFTokens, models.NewUDFToken(udf, 0))
	}

	ctx := context.Background()

	v, err := reader.Read(ctx)
	require.Nil(t, err)
	require.Equal(t, v, expectedUDFTokens[0])

	v, err = reader.Read(ctx)
	require.Nil(t, err)
	require.Equal(t, v, expectedUDFTokens[1])

	v, err = reader.Read(ctx)
	require.Equal(t, err, io.EOF)
	require.Nil(t, v)

	reader.Close()
}

func TestUDFReaderReadFailed(t *testing.T) {
	t.Parallel()

	mockUDFGetter := mocks.NewMockudfGetter(t)
	mockUDFGetter.EXPECT().GetUDFs().Return(
		nil,
		fmt.Errorf("error"),
	)

	reader := NewUDFReader(mockUDFGetter, slog.Default())
	require.NotNil(t, reader)

	v, err := reader.Read(context.Background())
	require.NotNil(t, err)
	require.Nil(t, v)
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
			"unable to testSet the '%s' field value of the type %T, target must be a struct",
			fieldName,
			target,
		))
	}
	rf := rv.FieldByName(fieldName)

	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func newExpectedPolicy() *a.ScanPolicy {
	expectedPolicy := &a.ScanPolicy{}
	expectedPolicy.FilterExpression = noMrtSetExpression()

	return expectedPolicy
}
