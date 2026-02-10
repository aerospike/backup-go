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
	"net"
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
	testAddress     = "localhost:3000"
)

var errTest = errors.New("test error")

func TestGet(t *testing.T) {
	t.Run("Success with TCP", func(t *testing.T) {
		// Create a listener to simulate a server
		var lc net.ListenConfig
		listener, err := lc.Listen(t.Context(), "tcp", "localhost:0")
		require.NoError(t, err)
		defer listener.Close()

		// Get the dynamically assigned port
		addr := listener.Addr().String()

		// Test the Get function
		conn, err := Get(t.Context(), "tcp", addr, testTimeout, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		conn.Close()
	})

	t.Run("Error with Invalid Address", func(t *testing.T) {
		conn, err := Get(t.Context(), "tcp", "invalid-address:invalid-port", testTimeout, nil)
		require.Error(t, err)
		require.Nil(t, conn)
	})

	t.Run("Error with Invalid Connection Type", func(t *testing.T) {
		conn, err := Get(t.Context(), "invalid-type", testAddress, testTimeout, nil)
		require.Error(t, err)
		require.Nil(t, conn)
	})
}

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
	response := make([]byte, 0)
	response = append(response, header...)
	response = append(response, bodyJSON...)
	readIndex := 0
	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
		if readIndex+len(b) > len(response) {
			b = b[:len(response)-readIndex]
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

func TestConnection_ReadInvalidMagic(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	header := make([]byte, 8)
	// Set an invalid magic number
	binary.BigEndian.PutUint32(header[:4], 0x12345678)
	binary.BigEndian.PutUint32(header[4:], 10)

	readIndex := 0
	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
		if readIndex+len(b) > len(header) {
			b = b[:len(header)-readIndex]
		}
		copy(b, header[readIndex:readIndex+len(b)])
		readIndex += len(b)
		return len(b), nil
	}).Once()

	_, err := Read(mockConn, testTimeout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic number")
}

func TestConnection_ReadBodyError(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], 10) // Body length

	readIndex := 0
	bodyError := errors.New("body read error")

	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
		if readIndex < len(header) {
			if readIndex+len(b) > len(header) {
				b = b[:len(header)-readIndex]
			}
			copy(b, header[readIndex:readIndex+len(b)])
			readIndex += len(b)
			return len(b), nil
		}
		return 0, bodyError
	}).Twice()

	_, err := Read(mockConn, testTimeout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read header")
}

func TestConnection_ReadInvalidJSON(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	header := make([]byte, 8)
	// Invalid JSON body
	body := []byte("invalid json")

	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], uint32(len(body)))
	response := make([]byte, 0)
	response = append(response, header...)
	response = append(response, body...)
	readIndex := 0

	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
		if readIndex+len(b) > len(response) {
			b = b[:len(response)-readIndex]
		}
		copy(b, response[readIndex:readIndex+len(b)])
		readIndex += len(b)
		return len(b), nil
	}).Twice()

	_, err := Read(mockConn, testTimeout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal response")
}

func TestConnection_ReadErrorInResponse(t *testing.T) {
	mockConn := new(mocks.Mockconnector)
	header := make([]byte, 8)
	body := models.Response{
		SecretValue: "",
		Error:       "error from server",
	}
	bodyJSON, err := json.Marshal(body)
	require.NoError(t, err)

	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], uint32(len(bodyJSON)))
	response := make([]byte, 0)
	response = append(response, header...)
	response = append(response, bodyJSON...)
	readIndex := 0

	mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
	mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
		if readIndex+len(b) > len(response) {
			b = b[:len(response)-readIndex]
		}
		copy(b, response[readIndex:readIndex+len(b)])
		readIndex += len(b)
		return len(b), nil
	}).Twice()

	_, err = Read(mockConn, testTimeout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error from server")
}

func TestReadBytes(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockConn := new(mocks.Mockconnector)
		data := []byte("test data")
		readIndex := 0

		mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
			if readIndex+len(b) > len(data) {
				b = b[:len(data)-readIndex]
			}
			copy(b, data[readIndex:readIndex+len(b)])
			readIndex += len(b)
			return len(b), nil
		})

		result, err := ReadBytes(mockConn, len(data))
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("Error", func(t *testing.T) {
		mockConn := new(mocks.Mockconnector)
		readError := errors.New("read error")

		mockConn.On("Read", mock.Anything).Return(0, readError)

		_, err := ReadBytes(mockConn, 10)
		require.Error(t, err)
		require.Equal(t, readError, err)
	})

	t.Run("Partial Read", func(t *testing.T) {
		mockConn := new(mocks.Mockconnector)
		data := []byte("test data")
		readIndex := 0
		callCount := 0

		mockConn.On("Read", mock.Anything).Return(func(b []byte) (int, error) {
			callCount++
			if callCount == 1 {
				// First call, return half the data
				copy(b, data[:len(data)/2])
				readIndex = len(data) / 2
				return len(data) / 2, nil
			}
			// Second call, return the rest
			copy(b, data[readIndex:])
			return len(data) - readIndex, nil
		})

		result, err := ReadBytes(mockConn, len(data))
		require.NoError(t, err)
		require.Equal(t, data, result)
	})
}
