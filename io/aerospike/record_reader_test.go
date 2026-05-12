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
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/scanlimiter"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/mock"
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

	ctx := t.Context()

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
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)

	require.NoError(t, err)
	expectedRecToken := models.NewRecordToken(mockRec, 0, nil)
	require.Equal(t, expectedRecToken, v)

	v, err = reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, v)

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

	ctx := t.Context()

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
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.Error(t, err)
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
	ctx := t.Context()

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
			scanLimiter:     scanlimiter.Noop,
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

	ctx := t.Context()

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
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.Error(t, err)
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

	ctx := t.Context()
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
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.NoError(t, err)
	expectedRecToken := models.NewRecordToken(mockRec, 0, nil)
	require.Equal(t, expectedRecToken, v)

	v, err = reader.Read(ctx)
	require.Equal(t, err, io.EOF)
	require.Nil(t, v)

	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderRetriesAfterThrottleError(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""

	throttleRecordSet := &a.Recordset{}
	throttleResults := make(chan *a.Result, 1)
	throttleResults <- &a.Result{
		Err: &a.AerospikeError{
			ResultCode: atypes.NO_AVAILABLE_CONNECTIONS_TO_NODE,
		},
	}
	close(throttleResults)
	setFieldValue(throttleRecordSet, "records", throttleResults)

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	okRecordSet := &a.Recordset{}
	okResults := make(chan *a.Result, 1)
	okRec := &a.Record{
		Bins: a.BinMap{
			"key": "hi",
		},
		Key: key,
	}
	okResults <- &a.Result{
		Record: okRec,
	}
	close(okResults)
	setFieldValue(okRecordSet, "records", okResults)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set).
		Return(throttleRecordSet, nil).Once()
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set).
		Return(okRecordSet, nil).Once()

	ctx := t.Context()
	throttler := NewThrottleLimiter(2, time.Millisecond)
	throttler.Notify(ctx) // ensure Wait() in retry path does not sleep.

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(throttleRecordSet).Return(nil).Once()
	closer.EXPECT().Close(okRecordSet).Return(nil).Once()
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			scanLimiter:     scanlimiter.Noop,
			throttler:       throttler,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, models.NewRecordToken(&models.Record{Record: okRec}, 0, nil), v)

	v, err = reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, v)
}

func TestAerospikeRecordReaderProgressesThroughMultipleSets(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set1 := "set1"
	set2 := "set2"

	key1, aerr := a.NewKey(namespace, set1, "key1")
	if aerr != nil {
		panic(aerr)
	}
	key2, aerr := a.NewKey(namespace, set2, "key2")
	if aerr != nil {
		panic(aerr)
	}

	rs1 := &a.Recordset{}
	results1 := make(chan *a.Result, 1)
	rec1 := &a.Record{Bins: a.BinMap{"k": "v1"}, Key: key1}
	results1 <- &a.Result{Record: rec1}
	close(results1)
	setFieldValue(rs1, "records", results1)

	rs2 := &a.Recordset{}
	results2 := make(chan *a.Result, 1)
	rec2 := &a.Record{Bins: a.BinMap{"k": "v2"}, Key: key2}
	results2 <- &a.Result{Record: rec2}
	close(results2)
	setFieldValue(rs2, "records", results2)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set1).
		Return(rs1, nil).Once()
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set2).
		Return(rs2, nil).Once()

	ctx := t.Context()
	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(rs1).Return(nil).Once()
	closer.EXPECT().Close(rs2).Return(nil).Once()
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set1, set2},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, models.NewRecordToken(&models.Record{Record: rec1}, 0, nil), v)

	v, err = reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, models.NewRecordToken(&models.Record{Record: rec2}, 0, nil), v)

	v, err = reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, v)
}

func TestAerospikeRecordReaderCancelledContextClosesActiveScan(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""

	recordset := &a.Recordset{}
	results := make(chan *a.Result)
	setFieldValue(recordset, "records", results)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set).
		Return(recordset, nil).Once()

	parentCtx := t.Context()
	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(recordset).Return(nil).Once()
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		parentCtx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(parentCtx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	readCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	v, err := reader.Read(readCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, v)
}

// TestAerospikeRecordReaderCloseReleasesActiveScan matches pipe: Read and RecordReader.Close
// run on the same goroutine (deferred Close after the read loop). After one record is
// read the scan is still open; pipe.Close must still release the recordset / scan slot.
func TestAerospikeRecordReaderCloseReleasesActiveScan(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""
	key, aerr := a.NewKey(namespace, set, "k")
	require.NoError(t, aerr)
	rec := &a.Record{Bins: a.BinMap{"x": 1}, Key: key}

	recordset := &a.Recordset{}
	results := make(chan *a.Result, 1)
	results <- &a.Result{Record: rec}
	// more results and drain left for a real close-from-recordset; Close() uses closer on active scan
	setFieldValue(recordset, "records", results)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set).
		Return(recordset, nil).Once()

	ctx := t.Context()
	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(recordset).Return(nil).Once()

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	_, rerr := reader.Read(ctx)
	require.NoError(t, rerr)
	reader.Close()

	closer.AssertExpectations(t)
	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderReturnsCloseErrorOnScanFinish(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""

	recordset := &a.Recordset{}
	results := make(chan *a.Result)
	close(results)
	setFieldValue(recordset, "records", results)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(newExpectedPolicy(), a.NewPartitionFilterByRange(0, 4096), namespace, set).
		Return(recordset, nil).Once()

	ctx := t.Context()
	closeErr := &a.AerospikeError{ResultCode: atypes.PARAMETER_ERROR}
	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(recordset).Return(closeErr).Once()
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			scanLimiter:     scanlimiter.Noop,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	v, err := reader.Read(ctx)
	require.ErrorIs(t, err, closeErr)
	require.Nil(t, v)
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
	mockSIndexGetter.EXPECT().GetSIndexes(mock.Anything, namespace).Return(
		mockSIndexes,
		nil,
	)

	reader := NewSIndexReader(mockSIndexGetter, namespace, slog.Default())
	require.NotNil(t, reader)

	expectedSIndexTokens := make([]*models.Token, 0, len(mockSIndexes))
	for _, sindex := range mockSIndexes {
		expectedSIndexTokens = append(expectedSIndexTokens, models.NewSIndexToken(sindex, 0))
	}

	ctx := t.Context()

	v, err := reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, v, expectedSIndexTokens[0])

	v, err = reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, v, expectedSIndexTokens[1])

	v, err = reader.Read(ctx)
	require.Equal(t, err, io.EOF)
	require.Nil(t, v)

	reader.Close()

	mockSIndexGetter.AssertExpectations(t)

	// negative GetSindexes fails

	mockSIndexGetter = mocks.NewMocksindexGetter(t)
	mockSIndexGetter.EXPECT().GetSIndexes(mock.Anything, namespace).Return(
		nil,
		fmt.Errorf("error"),
	)

	reader = NewSIndexReader(mockSIndexGetter, namespace, slog.Default())
	require.NotNil(t, reader)

	v, err = reader.Read(ctx)
	require.Error(t, err)
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
	mockUDFGetter.EXPECT().GetUDFs(mock.Anything).Return(
		mockUDFs,
		nil,
	)

	reader := NewUDFReader(mockUDFGetter, slog.Default())
	require.NotNil(t, reader)

	expectedUDFTokens := make([]*models.Token, 0, len(mockUDFs))
	for _, udf := range mockUDFs {
		expectedUDFTokens = append(expectedUDFTokens, models.NewUDFToken(udf, 0))
	}

	ctx := t.Context()

	v, err := reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, v, expectedUDFTokens[0])

	v, err = reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, v, expectedUDFTokens[1])

	v, err = reader.Read(ctx)
	require.Equal(t, err, io.EOF)
	require.Nil(t, v)

	reader.Close()
}

func TestUDFReaderReadFailed(t *testing.T) {
	t.Parallel()

	mockUDFGetter := mocks.NewMockudfGetter(t)
	mockUDFGetter.EXPECT().GetUDFs(mock.Anything).Return(
		nil,
		fmt.Errorf("error"),
	)

	reader := NewUDFReader(mockUDFGetter, slog.Default())
	require.NotNil(t, reader)

	v, err := reader.Read(t.Context())
	require.Error(t, err)
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
