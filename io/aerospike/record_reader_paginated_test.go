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
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/mocks"

	"context"
	"testing"

	"github.com/stretchr/testify/require"
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

	ctx := context.Background()

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
	require.Nil(t, err)
	require.NotNil(t, token1)
	require.Equal(t, "value1", token1.Record.Bins["key"])
	require.NotNil(t, token1.Filter) // Should have partition filter

	// Read second record
	token2, err := reader.Read(ctx)
	require.Nil(t, err)
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
			pageSize:        pageSize,
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
			pageSize:        pageSize,
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

	ctx := context.Background()

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
		require.Nil(t, err)
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
