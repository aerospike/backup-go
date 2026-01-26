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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestAerospikeRecordReaderPaginated(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(2)

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	// First page - 2 records
	mockRecordSet1 := &a.Recordset{}
	mockResults1 := make(chan *a.Result, 2)
	rec1 := &a.Record{
		Bins: a.BinMap{"key": "value1"},
		Key:  key,
	}
	rec2 := &a.Record{
		Bins: a.BinMap{"key": "value2"},
		Key:  key,
	}
	mockResults1 <- &a.Result{Record: rec1}
	mockResults1 <- &a.Result{Record: rec2}
	close(mockResults1)
	setFieldValue(mockRecordSet1, "records", mockResults1)

	// Second page - empty (end of scan)
	mockRecordSet2 := &a.Recordset{}
	mockResults2 := make(chan *a.Result, 1)
	close(mockResults2)
	setFieldValue(mockRecordSet2, "records", mockResults2)

	pf := a.NewPartitionFilterAll()
	expectedPolicy := newExpectedPaginatedPolicy(pageSize)

	mockScanner := mocks.NewMockscanner(t)
	// First call returns 2 records
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		pf,
		namespace,
		set,
	).Return(mockRecordSet1, nil).Once()

	// Second call returns empty recordset (end of pagination)
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		pf,
		namespace,
		set,
	).Return(mockRecordSet2, nil).Once()

	ctx := t.Context()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet1).Return(nil)
	closer.EXPECT().Close(mockRecordSet2).Return(nil)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: pf,
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Read first record
	token1, err := reader.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, token1)
	require.Equal(t, "value1", token1.Record.Bins["key"])
	require.NotNil(t, token1.Filter) // Should have partition filter

	// Read second record
	token2, err := reader.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, token2)
	require.Equal(t, "value2", token2.Record.Bins["key"])
	require.NotNil(t, token2.Filter)

	// Should reach EOF
	token3, err := reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, token3)

	mockScanner.AssertExpectations(t)
}

func TestAerospikeRecordReaderPaginatedRecordError(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(1)

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	mockRec := &a.Record{
		Bins: a.BinMap{"key": "hi"},
		Key:  key,
	}
	mockResults <- &a.Result{
		Record: mockRec,
		Err:    a.ErrInvalidParam,
	}
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set,
	).Return(mockRecordSet, nil)

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
			pageSize:        pageSize,
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

func TestAerospikeRecordReaderPaginatedScanFailed(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(1)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set,
	).Return(nil, a.ErrInvalidParam)

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
			pageSize:        pageSize,
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

func TestAerospikeRecordReaderPaginatedMultipleSets(t *testing.T) {
	namespace := "test"
	set1 := "set1"
	set2 := "set2"
	pageSize := int64(1)

	key, aerr := a.NewKey(namespace, set1, "key")
	if aerr != nil {
		panic(aerr)
	}

	// Mock recordsets for each set
	mockRecordSet1 := &a.Recordset{}
	mockResults1 := make(chan *a.Result, 1)
	rec1 := &a.Record{
		Bins: a.BinMap{"set": "set1_record"},
		Key:  key,
	}
	mockResults1 <- &a.Result{Record: rec1}
	close(mockResults1)
	setFieldValue(mockRecordSet1, "records", mockResults1)

	// Empty recordset to end set1 pagination
	mockRecordSet1End := &a.Recordset{}
	mockResults1End := make(chan *a.Result)
	close(mockResults1End)
	setFieldValue(mockRecordSet1End, "records", mockResults1End)

	mockRecordSet2 := &a.Recordset{}
	mockResults2 := make(chan *a.Result, 1)
	rec2 := &a.Record{
		Bins: a.BinMap{"set": "set2_record"},
		Key:  key,
	}
	mockResults2 <- &a.Result{Record: rec2}
	close(mockResults2)
	setFieldValue(mockRecordSet2, "records", mockResults2)

	// Empty recordset to end set2 pagination
	mockRecordSet2End := &a.Recordset{}
	mockResults2End := make(chan *a.Result)
	close(mockResults2End)
	setFieldValue(mockRecordSet2End, "records", mockResults2End)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)

	mockScanner := mocks.NewMockscanner(t)
	// Note: Order may vary due to shuffling, but we expect these calls
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set1,
	).Return(mockRecordSet1, nil).Once()
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set1,
	).Return(mockRecordSet1End, nil).Once()
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set2,
	).Return(mockRecordSet2, nil).Once()
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set2,
	).Return(mockRecordSet2End, nil).Once()

	ctx := t.Context()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet1).Return(nil)
	closer.EXPECT().Close(mockRecordSet1End).Return(nil)
	closer.EXPECT().Close(mockRecordSet2).Return(nil)
	closer.EXPECT().Close(mockRecordSet2End).Return(nil)
	defer closer.AssertExpectations(t)

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set1, set2},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector: metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond,
				testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Read records from both sets
	recordsRead := 0
	setNames := make(map[string]bool)

	for recordsRead < 2 {
		token, err := reader.Read(ctx)
		require.NoError(t, err)
		require.NotNil(t, token)
		require.NotNil(t, token.Filter)

		setName := token.Record.Bins["set"].(string)
		setNames[setName] = true
		recordsRead++
	}

	// Should have read from both sets
	require.True(t, setNames["set1_record"])
	require.True(t, setNames["set2_record"])

	// Should reach EOF
	token, err := reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, token)

	mockScanner.AssertExpectations(t)
}

// Helper function to create expected paginated policy
func newExpectedPaginatedPolicy(pageSize int64) *a.ScanPolicy {
	expectedPolicy := &a.ScanPolicy{}
	expectedPolicy.MaxRecords = pageSize
	expectedPolicy.FilterExpression = noMrtSetExpression()
	return expectedPolicy
}

// Test context cancellation during pagination
func TestAerospikeRecordReaderPaginatedContextCanceled(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(2)

	mockScanner := mocks.NewMockscanner(t)
	closer := mocks.NewMockRecordsetCloser(t)

	ctx, cancel := context.WithCancel(t.Context())

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector:    metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond, testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Cancel context before reading
	cancel()

	token, err := reader.Read(ctx)
	require.Equal(t, context.Canceled, err)
	require.Nil(t, token)
}

// Test pagination with scan limiter (semaphore)
func TestAerospikeRecordReaderPaginatedWithScanLimiter(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(1)

	key, aerr := a.NewKey(namespace, set, "key")
	require.NoError(t, aerr)

	// Create recordset with one record
	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	rec := &a.Record{
		Bins: a.BinMap{"key": "value"},
		Key:  key,
	}
	mockResults <- &a.Result{Record: rec}
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	// Empty recordset for end of scan
	mockRecordSetEnd := &a.Recordset{}
	mockResultsEnd := make(chan *a.Result)
	close(mockResultsEnd)
	setFieldValue(mockRecordSetEnd, "records", mockResultsEnd)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)
	pf := a.NewPartitionFilterAll()

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSet, nil).Once()
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSetEnd, nil).Once()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	closer.EXPECT().Close(mockRecordSetEnd).Return(nil)

	ctx := t.Context()
	scanLimiter := semaphore.NewWeighted(1) // Only allow 1 concurrent scan

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: pf,
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			scanLimiter:     scanLimiter,
			rpsCollector:    metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond, testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Read record
	token, err := reader.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, token)
	require.Equal(t, "value", token.Record.Bins["key"])

	// Should reach EOF
	token, err = reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, token)

	mockScanner.AssertExpectations(t)
}

// Test pagination with INVALID_NODE_ERROR (should be ignored)
func TestAerospikeRecordReaderPaginatedIgnoreInvalidNodeError(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(2)

	key, aerr := a.NewKey(namespace, set, "key")
	require.NoError(t, aerr)

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 2)

	// First record is valid
	rec1 := &a.Record{
		Bins: a.BinMap{"key": "value1"},
		Key:  key,
	}
	mockResults <- &a.Result{Record: rec1}

	// Second result has INVALID_NODE_ERROR (should be ignored)
	mockResults <- &a.Result{
		Err: a.ErrClusterIsEmpty,
	}
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	// Empty recordset for end of scan
	mockRecordSetEnd := &a.Recordset{}
	mockResultsEnd := make(chan *a.Result)
	close(mockResultsEnd)
	setFieldValue(mockRecordSetEnd, "records", mockResultsEnd)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)
	pf := a.NewPartitionFilterAll()

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSet, nil).Once()
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSetEnd, nil).Once()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	closer.EXPECT().Close(mockRecordSetEnd).Return(nil)

	ctx := t.Context()

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: pf,
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector:    metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond, testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Should read valid record (invalid node error ignored)
	token, err := reader.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, token)
	require.Equal(t, "value1", token.Record.Bins["key"])

	// Should reach EOF
	token, err = reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, token)

	mockScanner.AssertExpectations(t)
}

// Test pagination with recordset close error
func TestAerospikeRecordReaderPaginatedRecordsetCloseError(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(1)

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result)
	close(mockResults) // Empty recordset
	setFieldValue(mockRecordSet, "records", mockResults)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(
		expectedPolicy,
		a.NewPartitionFilterAll(),
		namespace,
		set,
	).Return(mockRecordSet, nil).Once()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(a.ErrNetwork).Once()

	ctx := t.Context()

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: a.NewPartitionFilterAll(),
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector:    metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond, testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	token, err := reader.Read(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to close record set")
	require.Nil(t, token)

	mockScanner.AssertExpectations(t)
}

// Test pagination with large page size
func TestAerospikeRecordReaderPaginatedLargePageSize(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(10000) // Large page size

	key, aerr := a.NewKey(namespace, set, "key")
	require.NoError(t, aerr)

	// Create recordset with several records
	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 5)
	for i := range 5 {
		rec := &a.Record{
			Bins: a.BinMap{"key": fmt.Sprintf("value%d", i)},
			Key:  key,
		}
		mockResults <- &a.Result{Record: rec}
	}
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	// Empty recordset for end
	mockRecordSetEnd := &a.Recordset{}
	mockResultsEnd := make(chan *a.Result)
	close(mockResultsEnd)
	setFieldValue(mockRecordSetEnd, "records", mockResultsEnd)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)
	pf := a.NewPartitionFilterAll()

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSet, nil).Once()
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSetEnd, nil).Once()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	closer.EXPECT().Close(mockRecordSetEnd).Return(nil)

	ctx := t.Context()

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: pf,
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector:    metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond, testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Read all 5 records
	for i := range 5 {
		token, err := reader.Read(ctx)
		require.NoError(t, err)
		require.NotNil(t, token)
		require.Equal(t, fmt.Sprintf("value%d", i), token.Record.Bins["key"])
	}

	// Should reach EOF
	token, err := reader.Read(ctx)
	require.Equal(t, io.EOF, err)
	require.Nil(t, token)

	mockScanner.AssertExpectations(t)
}

// Test concurrent reads (should be safe)
func TestAerospikeRecordReaderPaginatedConcurrentReads(t *testing.T) {
	namespace := "test"
	set := ""
	pageSize := int64(1)

	key, aerr := a.NewKey(namespace, set, "key")
	require.NoError(t, aerr)

	mockRecordSet := &a.Recordset{}
	mockResults := make(chan *a.Result, 1)
	rec := &a.Record{
		Bins: a.BinMap{"key": "value"},
		Key:  key,
	}
	mockResults <- &a.Result{Record: rec}
	close(mockResults)
	setFieldValue(mockRecordSet, "records", mockResults)

	mockRecordSetEnd := &a.Recordset{}
	mockResultsEnd := make(chan *a.Result)
	close(mockResultsEnd)
	setFieldValue(mockRecordSetEnd, "records", mockResultsEnd)

	expectedPolicy := newExpectedPaginatedPolicy(pageSize)
	pf := a.NewPartitionFilterAll()

	mockScanner := mocks.NewMockscanner(t)
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSet, nil).Once()
	mockScanner.EXPECT().ScanPartitions(expectedPolicy, pf, namespace, set).Return(mockRecordSetEnd, nil).Once()

	closer := mocks.NewMockRecordsetCloser(t)
	closer.EXPECT().Close(mockRecordSet).Return(nil)
	closer.EXPECT().Close(mockRecordSetEnd).Return(nil)

	ctx := t.Context()

	reader := NewRecordReader(
		ctx,
		mockScanner,
		&RecordReaderConfig{
			namespace:       namespace,
			setList:         []string{set},
			partitionFilter: pf,
			scanPolicy:      &a.ScanPolicy{},
			pageSize:        pageSize,
			rpsCollector:    metrics.NewCollector(ctx, slog.Default(), metrics.RecordsPerSecond, testMetricMessage, true),
		},
		slog.Default(),
		closer,
	)
	require.NotNil(t, reader)

	// Try to read concurrently (should be handled gracefully)
	done := make(chan bool, 2)
	var tokens []*models.Token
	var errs []error
	var mu sync.Mutex

	for range 2 {
		go func() {
			defer func() { done <- true }()
			token, err := reader.Read(ctx)
			mu.Lock()
			tokens = append(tokens, token)
			errs = append(errs, err)
			mu.Unlock()
		}()
	}

	// Wait for both goroutines
	<-done
	<-done

	// One should get the record, one should get EOF or nil
	mu.Lock()
	require.Len(t, tokens, 2)
	require.Len(t, errs, 2)

	validTokens := 0
	eofErrors := 0
	for i := range 2 {
		if tokens[i] != nil {
			validTokens++
		}
		if errors.Is(errs[i], io.EOF) {
			eofErrors++
		}
	}

	require.Equal(t, 1, validTokens, "Should have exactly one valid token")
	require.Equal(t, 1, eofErrors, "Should have exactly one EOF")
	mu.Unlock()

	mockScanner.AssertExpectations(t)
}
