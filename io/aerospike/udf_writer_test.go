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

const testUdfName = "test.lua"

var testUdfContent = []byte("function test() return 1 end")

func TestUDFWriterInvalidType(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)

	logger := slog.Default()

	writer := udfWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	udf := &models.UDF{
		Name:    testUdfName,
		Content: testUdfContent,
		UDFType: 'X',
	}

	err := writer.writeUDF(udf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid UDF language")

	mockDBWriter.AssertExpectations(t)
}

func TestUDFWriterRegisterError(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)
	mockDBWriter.EXPECT().RegisterUDF(
		mock.Anything,
		testUdfContent,
		testUdfName,
		a.LUA,
	).Return(nil, a.ErrInvalidParam)

	logger := slog.Default()

	writer := udfWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	udf := &models.UDF{
		Name:    testUdfName,
		Content: testUdfContent,
		UDFType: models.UDFTypeLUA,
	}

	err := writer.writeUDF(udf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error registering UDF")

	mockDBWriter.AssertExpectations(t)
}

func TestUDFWriterNilJob(t *testing.T) {
	t.Parallel()

	mockDBWriter := mocks.NewMockdbWriter(t)
	mockDBWriter.EXPECT().RegisterUDF(
		mock.Anything,
		testUdfContent,
		testUdfName,
		a.LUA,
	).Return(nil, nil)

	logger := slog.Default()

	writer := udfWriter{
		asc:         mockDBWriter,
		writePolicy: &a.WritePolicy{},
		logger:      logger,
	}

	udf := &models.UDF{
		Name:    testUdfName,
		Content: testUdfContent,
		UDFType: models.UDFTypeLUA,
	}

	err := writer.writeUDF(udf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "job is nil")

	mockDBWriter.AssertExpectations(t)
}
