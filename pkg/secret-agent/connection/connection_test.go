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

package connection

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/secret-agent/connection/mocks"
	"github.com/aerospike/backup-go/pkg/secret-agent/models"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testResource  = "resource"
	testSecretKey = "secret-key"
	//nolint:gosec // it is not hardcoded credentials, it is test hash string.
	testSecretValue = "e1518262adfdd22c3617c8a606f24c02a18f19aae1d300903e0e4c05eae8ba70"
	testTimeout     = 10 * time.Second
)

var errTest = errors.New("test error")

func TestConnection_WriteOK(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	mockConn.On("Write", mock.Anything).Return(42, nil)
	mockConn.On("SetWriteDeadline", mock.Anything).Return(nil)

	err := Write(mockConn, testTimeout, testResource, testSecretKey)
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
}

func TestConnection_WriteError(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	mockConn.On("SetWriteDeadline", mock.Anything).Return(nil)
	mockConn.On("Write", mock.Anything).Return(0, errTest)

	err := Write(mockConn, testTimeout, testResource, testSecretKey)
	require.ErrorIs(t, err, errTest)

	mockConn.AssertExpectations(t)
}

func TestConnection_WriteTimeout(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	mockConn.On("SetWriteDeadline", mock.Anything).Return(errTest)

	err := Write(mockConn, testTimeout, testResource, testSecretKey)
	require.ErrorIs(t, err, errTest)

	mockConn.AssertExpectations(t)
}

func TestConnection_ReadOK(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	header := make([]byte, 8)
	body := models.Response{
		SecretValue: testSecretValue,
		Error:       "",
	}
	bodyJSON, err := json.Marshal(body)
	require.NoError(t, err)

	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], uint32(len(bodyJSON)))
	//nolint:gocritic // we don't need to append result to same slice. I need new slice for test.
	response := append(header, bodyJSON...)
	readIndex := 0
	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
		if readIndex+len(b) > len(response) {
			b = b[:len(response)-readIndex] // Адаптируем размер, если выходим за пределы
		}
		copy(b, response[readIndex:readIndex+len(b)])
		readIndex += len(b)
		return len(b), nil
	}).Twice()

	secret, err := Read(mockConn, testTimeout)
	require.NoError(t, err)
	require.Equal(t, testSecretValue, secret)

	mockConn.AssertExpectations(t)
}

func TestConnection_ReadError(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	headerError := errors.New("header read error")
	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(0, headerError).Once()

	_, err := Read(mockConn, testTimeout)
	require.ErrorIs(t, err, headerError)

	mockConn.AssertExpectations(t)
}

func TestConnection_ReadTimeout(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	mockConn.On("SetReadDeadline", mock.Anything).Return(errTest)

	_, err := Read(mockConn, testTimeout)
	require.ErrorIs(t, err, errTest)

	mockConn.AssertExpectations(t)
}
