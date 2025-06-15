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
package xdr

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/require"
)

// mockConn implements net.Conn interface for testing
type mockConn struct {
	readData  []byte
	readIndex int
	writeData []byte
	closed    bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("connection closed")
	}
	if m.readIndex >= len(m.readData) {
		return 0, errors.New("EOF")
	}
	n = copy(b, m.readData[m.readIndex:])
	m.readIndex += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("connection closed")
	}
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(_ time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestParserRead(t *testing.T) {
	t.Parallel()
	validBody, err := base64.StdEncoding.DecodeString(testMessageB64)
	require.NoError(t, err)

	validMsg := NewPayload(validBody)

	t.Run("SuccessfulRead", func(t *testing.T) {
		conn := &mockConn{readData: validMsg}
		parser := NewParser(conn)

		body, err := parser.Read()
		require.NoError(t, err)
		require.Equal(t, validBody, body)
	})

	t.Run("InvalidVersion", func(t *testing.T) {
		t.Parallel()
		invalidVersion := make([]byte, len(validMsg))
		copy(invalidVersion, validMsg)
		invalidVersion[0] = 3 // Change version to 3

		conn := &mockConn{readData: invalidVersion}
		parser := NewParser(conn)

		_, err := parser.Read()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported protocol version")
	})

	t.Run("InvalidType", func(t *testing.T) {
		t.Parallel()
		invalidType := make([]byte, len(validMsg))
		copy(invalidType, validMsg)
		invalidType[1] = 4 // Change type to 4

		conn := &mockConn{readData: invalidType}
		parser := NewParser(conn)

		_, err := parser.Read()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid proto type")
	})

	t.Run("InvalidSize", func(t *testing.T) {
		t.Parallel()
		invalidSize := make([]byte, len(validMsg))
		copy(invalidSize, validMsg)
		// Set size to a very large value
		invalidSize[2] = 0xFF
		invalidSize[3] = 0xFF
		invalidSize[4] = 0xFF
		invalidSize[5] = 0xFF
		invalidSize[6] = 0xFF
		invalidSize[7] = 0xFF

		conn := &mockConn{readData: invalidSize}
		parser := NewParser(conn)

		_, err := parser.Read()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid proto size bytes")
	})

	t.Run("ReadError", func(t *testing.T) {
		t.Parallel()
		conn := &mockConn{closed: true}
		parser := NewParser(conn)

		_, err := parser.Read()
		require.Error(t, err)
	})
}

func TestParseAerospikeMessage(t *testing.T) {
	t.Parallel()
	validBody, err := base64.StdEncoding.DecodeString(testMessageB64)
	require.NoError(t, err)

	t.Run("SuccessfulParse", func(t *testing.T) {
		t.Parallel()
		msg, err := ParseAerospikeMessage(validBody)
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.Equal(t, int8(22), msg.HeaderSize) // Header size should be 22
		require.Equal(t, int16(5), msg.NumFields)  // Number of fields should be 5
	})

	t.Run("MessageTooShort", func(t *testing.T) {
		t.Parallel()
		_, err := ParseAerospikeMessage([]byte{1, 2, 3})
		require.Error(t, err)
		require.Contains(t, err.Error(), "message too short")
	})
}

func TestParseFields(t *testing.T) {
	t.Parallel()
	validBody, err := base64.StdEncoding.DecodeString(testMessageB64)
	require.NoError(t, err)

	msg, err := ParseAerospikeMessage(validBody)
	require.NoError(t, err)

	t.Run("EmptyMessage", func(t *testing.T) {
		t.Parallel()
		fields := ParseFields([]byte{}, 0)
		require.Nil(t, fields)
	})

	t.Run("ValidFields", func(t *testing.T) {
		t.Parallel()
		require.NotNil(t, msg.Fields)
		require.Len(t, msg.Fields, 5)
	})
}

func TestParseField(t *testing.T) {
	t.Parallel()
	t.Run("EmptyField", func(t *testing.T) {
		t.Parallel()
		field, end := ParseField([]byte{})
		require.Nil(t, field)
		require.Equal(t, int32(0), end)
	})

	t.Run("ValidField", func(t *testing.T) {
		t.Parallel()
		// Create a field with size 10, type 1, and some data
		fieldData := []byte{0, 0, 0, 10, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		field, end := ParseField(fieldData)
		require.NotNil(t, field)
		require.Equal(t, int32(10), field.Size)
		require.Equal(t, int8(1), field.Type)
		require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, field.Data)
		require.Equal(t, int32(14), end)
	})
}

func TestNewPayload(t *testing.T) {
	t.Parallel()
	body := []byte{1, 2, 3, 4, 5}
	payload := NewPayload(body)

	require.Equal(t, byte(ProtoVersion), payload[0])
	require.Equal(t, byte(ProtoTypeMessage), payload[1])
	require.Equal(t, body, payload[LenProtoHeader:])

	size := fieldToInt64(payload[2:8])
	require.Equal(t, int64(len(body)), size)
}

func TestResetXDRBit(t *testing.T) {
	t.Parallel()
	msg := []byte{0, MsgInfo1Xdr, 0, 0, 0}
	result := ResetXDRBit(msg)

	require.Equal(t, byte(0), result[1]&MsgInfo1Xdr)
}

func TestSetGenerationBit(t *testing.T) {
	t.Parallel()

	t.Run("NonePolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetGenerationBit(aerospike.NONE, msg)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2Generation)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2GenerationGt)
	})

	t.Run("ExpectGenGtPolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetGenerationBit(aerospike.EXPECT_GEN_GT, msg)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2Generation)
		require.Equal(t, byte(MsgInfo2GenerationGt), result[info2Pos+LenProtoHeader]&MsgInfo2GenerationGt)
	})

	t.Run("ExpectGenEqualPolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetGenerationBit(aerospike.EXPECT_GEN_EQUAL, msg)
		require.Equal(t, byte(MsgInfo2Generation), result[info2Pos+LenProtoHeader]&MsgInfo2Generation)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2GenerationGt)
	})
}

func TestSetRecordExistsActionBit(t *testing.T) {
	t.Parallel()

	t.Run("UpdatePolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetRecordExistsActionBit(aerospike.UPDATE, msg)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3UpdateOnly)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3CreateOrReplace)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3ReplaceOnly)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2CreateOnly)
	})

	t.Run("CreateOnlyPolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetRecordExistsActionBit(aerospike.CREATE_ONLY, msg)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3UpdateOnly)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3CreateOrReplace)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3ReplaceOnly)
		require.Equal(t, byte(MsgInfo2CreateOnly), result[info2Pos+LenProtoHeader]&MsgInfo2CreateOnly)
	})

	t.Run("ReplacePolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetRecordExistsActionBit(aerospike.REPLACE, msg)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3UpdateOnly)
		require.Equal(t, byte(MsgInfo3CreateOrReplace), result[info3Pos+LenProtoHeader]&MsgInfo3CreateOrReplace)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3ReplaceOnly)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2CreateOnly)
	})

	t.Run("ReplaceOnlyPolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetRecordExistsActionBit(aerospike.REPLACE_ONLY, msg)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3UpdateOnly)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3CreateOrReplace)
		require.Equal(t, byte(MsgInfo3ReplaceOnly), result[info3Pos+LenProtoHeader]&MsgInfo3ReplaceOnly)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2CreateOnly)
	})

	t.Run("UpdateOnlyPolicy", func(t *testing.T) {
		t.Parallel()
		msg := NewPayload([]byte{0, 0, 0, 0, 0})
		result := SetRecordExistsActionBit(aerospike.UPDATE_ONLY, msg)
		require.Equal(t, byte(MsgInfo3UpdateOnly), result[info3Pos+LenProtoHeader]&MsgInfo3UpdateOnly)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3CreateOrReplace)
		require.Equal(t, byte(0), result[info3Pos+LenProtoHeader]&MsgInfo3ReplaceOnly)
		require.Equal(t, byte(0), result[info2Pos+LenProtoHeader]&MsgInfo2CreateOnly)
	})
}

func TestNewAckMessage(t *testing.T) {
	t.Parallel()
	t.Run("OkAck", func(t *testing.T) {
		t.Parallel()
		ack := NewAckMessage(AckOK)
		require.Equal(t, byte(ProtoVersion), ack[0])
		require.Equal(t, byte(ProtoTypeMessage), ack[1])
		require.Equal(t, byte(AckOK), ack[LenProtoHeader+5])
	})

	t.Run("RetryAck", func(t *testing.T) {
		t.Parallel()
		ack := NewAckMessage(AckRetry)
		require.Equal(t, byte(ProtoVersion), ack[0])
		require.Equal(t, byte(ProtoTypeMessage), ack[1])
		require.Equal(t, byte(AckRetry), ack[LenProtoHeader+5])
	})
}

func TestNewAerospikeKey(t *testing.T) {
	t.Parallel()
	t.Run("NotEnoughFields", func(t *testing.T) {
		t.Parallel()
		_, err := NewAerospikeKey([]*Field{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not enough fields data")
	})

	t.Run("MonitorRecordsSet", func(t *testing.T) {
		t.Parallel()
		fields := []*Field{
			{Type: FieldTypeNamespace, Data: []byte("test")},
			{Type: FieldTypeSet, Data: []byte(models.MonitorRecordsSetName)},
			{Type: FieldTypeDigest, Data: []byte{1, 2, 3, 4}},
		}
		_, err := NewAerospikeKey(fields)
		require.Error(t, err)
		require.Equal(t, errSkipRecord, err)
	})

	t.Run("ValidKeyWithStringUserKey", func(t *testing.T) {
		t.Parallel()
		fields := []*Field{
			{Type: FieldTypeNamespace, Data: []byte("test")},
			{Type: FieldTypeSet, Data: []byte("testset")},
			{Type: FieldTypeUserKey, Data: []byte{UserKeyTypeString, 'a', 'b', 'c'}},
			{Type: FieldTypeDigest, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
		}
		key, err := NewAerospikeKey(fields)
		require.NoError(t, err)
		require.Equal(t, "test", key.Namespace())
		require.Equal(t, "testset", key.SetName())
		require.Contains(t, key.String(), "abc")
	})

	t.Run("ValidKeyWithIntUserKey", func(t *testing.T) {
		t.Parallel()
		fields := []*Field{
			{Type: FieldTypeNamespace, Data: []byte("test")},
			{Type: FieldTypeSet, Data: []byte("testset")},
			{Type: FieldTypeUserKey, Data: []byte{UserKeyTypeInt, 0, 0, 0, 123}},
			{Type: FieldTypeDigest, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
		}
		key, err := NewAerospikeKey(fields)
		require.NoError(t, err)
		require.Equal(t, "test", key.Namespace())
		require.Equal(t, "testset", key.SetName())
		// Check that the key contains the string representation of the user key
		require.Contains(t, key.String(), "123")
	})

	t.Run("ValidKeyWithBlobUserKey", func(t *testing.T) {
		t.Parallel()
		blobData := []byte{1, 2, 3, 4}
		fields := []*Field{
			{Type: FieldTypeNamespace, Data: []byte("test")},
			{Type: FieldTypeSet, Data: []byte("testset")},
			{Type: FieldTypeUserKey, Data: append([]byte{UserKeyTypeBlob}, blobData...)},
			{Type: FieldTypeDigest, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
		}
		key, err := NewAerospikeKey(fields)
		require.NoError(t, err)
		require.Equal(t, "test", key.Namespace())
		require.Equal(t, "testset", key.SetName())
	})

	t.Run("UnsupportedKeyType", func(t *testing.T) {
		t.Parallel()
		fields := []*Field{
			{Type: FieldTypeNamespace, Data: []byte("test")},
			{Type: FieldTypeSet, Data: []byte("testset")},
			{Type: FieldTypeUserKey, Data: []byte{99, 1, 2, 3}}, // 99 is an invalid key type
			{Type: FieldTypeDigest, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
		}
		_, err := NewAerospikeKey(fields)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported key type")
	})
}

func TestFieldToInt64(t *testing.T) {
	t.Parallel()
	t.Run("EmptyInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt64([]byte{})
		require.Equal(t, int64(0), result)
	})

	t.Run("ValidInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt64([]byte{0, 0, 0, 0, 0, 123})
		require.Equal(t, int64(123), result)
	})
}

func TestFieldToInt32(t *testing.T) {
	t.Parallel()
	t.Run("EmptyInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt32([]byte{})
		require.Equal(t, int32(0), result)
	})

	t.Run("ValidInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt32([]byte{0, 0, 0, 123})
		require.Equal(t, int32(123), result)
	})
}

func TestFieldToInt16(t *testing.T) {
	t.Parallel()
	t.Run("EmptyInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt16([]byte{})
		require.Equal(t, int16(0), result)
	})

	t.Run("ValidInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt16([]byte{0, 123})
		require.Equal(t, int16(123), result)
	})
}

func TestFieldToInt8(t *testing.T) {
	t.Parallel()
	t.Run("EmptyInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt8([]byte{})
		require.Equal(t, int8(0), result)
	})

	t.Run("ValidInput", func(t *testing.T) {
		t.Parallel()
		result := fieldToInt8([]byte{123})
		require.Equal(t, int8(123), result)
	})
}

func TestReadBytes(t *testing.T) {
	t.Parallel()
	t.Run("SuccessfulRead", func(t *testing.T) {
		t.Parallel()
		data := []byte{1, 2, 3, 4, 5}
		conn := &mockConn{readData: data}
		result, err := readBytes(conn, int64(len(data)))
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("ReadError", func(t *testing.T) {
		t.Parallel()
		conn := &mockConn{closed: true}
		_, err := readBytes(conn, 5)
		require.Error(t, err)
	})

	t.Run("PartialRead", func(t *testing.T) {
		t.Parallel()
		data := []byte{1, 2, 3}
		conn := &mockConn{readData: data}
		_, err := readBytes(conn, 5)
		require.Error(t, err)
	})
}

func TestSetLength(t *testing.T) {
	t.Parallel()
	msg := make([]byte, 8)
	setLength(msg, 123456789)

	size := fieldToInt64(msg[2:8])
	require.Equal(t, int64(123456789), size)
}
