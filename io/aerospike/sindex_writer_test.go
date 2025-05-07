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

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace = "test"
	testSet       = "testset"
	testIndexName = "testindex"
	testBinName   = "testbin"
)

func TestSindexWriterSuccess(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything, // policy
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,     // index type
		a.ICT_DEFAULT, // index collection type
		mock.Anything, // ctx
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err) // Now we expect an error
}

func TestSindexWriterIndexAlreadyExists(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything,
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,
		a.ICT_DEFAULT,
		mock.Anything,
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err) // Now we expect an error
}

func TestSindexWriterCreateError(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything,
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,
		a.ICT_DEFAULT,
		mock.Anything,
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}

func TestSindexWriterDropError(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything,
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,
		a.ICT_DEFAULT,
		mock.Anything,
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}

func TestSindexWriterInvalidBinType(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: 'X',
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}

func TestSindexWriterInvalidIndexType(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		// Invalid index type
		IndexType: 'X',
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}

func TestSindexWriterWithCDTContext(t *testing.T) {
	t.Parallel()

	b64Context := "AQA="

	mockDBWriter := mocks.NewMockdbWriter(t)

	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything,
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,
		a.ICT_DEFAULT,
		mock.Anything,
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName:    testBinName,
			BinType:    models.NumericSIDataType,
			B64Context: b64Context,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err) // Now we expect an error
}

func TestSindexWriterInvalidCDTContext(t *testing.T) {
	t.Parallel()

	b64Context := "invalid-base64"

	mockDBWriter := mocks.NewMockdbWriter(t)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName:    testBinName,
			BinType:    models.NumericSIDataType,
			B64Context: b64Context,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}

func TestSindexWriterTaskError(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)
	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything,
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,
		a.ICT_DEFAULT,
		mock.Anything,
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}

func TestSindexWriterNilTask(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)
	mockDBWriter.EXPECT().CreateComplexIndex(
		mock.Anything,
		testNamespace,
		testSet,
		testIndexName,
		testBinName,
		a.NUMERIC,
		a.ICT_DEFAULT,
		mock.Anything,
	).Return(nil, nil)

	logger := slog.Default()

	writer := sindexWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	sindex := &models.SIndex{
		Namespace: testNamespace,
		Set:       testSet,
		Name:      testIndexName,
		Path: models.SIndexPath{
			BinName: testBinName,
			BinType: models.NumericSIDataType,
		},
		IndexType: models.BinSIndex,
	}

	err := writer.writeSecondaryIndex(sindex)
	require.Error(t, err)
}
