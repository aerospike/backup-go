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
	"fmt"
	"net"

	"github.com/aerospike/aerospike-client-go/v7"
)

const (
	ProtoVersion     = 2
	ProtoTypeMessage = 3

	LenMessageHeader = 22
	LenProtoHeader   = 8

	MaxProtoBody = 128 * 1024 * 1024
)

const (
	AckOK    = 0
	AckRetry = 11
)

type Parser struct {
	conn net.Conn
}

func NewParser(conn net.Conn) *Parser {
	return &Parser{
		conn: conn,
	}
}

// Read reads messages from connection.
// Starting from proto header, version, and message size.
// Then returns raw bytes with proto body.
func (p *Parser) Read() ([]byte, error) {
	versionBytes, err := readBytes(p.conn, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to read proto version: %w", err)
	}

	version := fieldToInt(versionBytes)
	if version != ProtoVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", version)
	}

	typeBytes, err := readBytes(p.conn, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to read proto type: %w", err)
	}

	protoType := fieldToInt(typeBytes)
	if protoType != ProtoTypeMessage {
		return nil, fmt.Errorf("invalid proto type: %d", protoType)
	}

	sizeBytes, err := readBytes(p.conn, 6)
	if err != nil {
		return nil, fmt.Errorf("failed to read proto size bytes: %w", err)
	}

	size := fieldToInt(sizeBytes)
	if size > MaxProtoBody {
		return nil, fmt.Errorf("invalid proto size bytes: %d", size)
	}

	bodyBytes, err := readBytes(p.conn, size)
	if err != nil {
		return nil, fmt.Errorf("failed to read proto body bytes: %w", err)
	}

	return bodyBytes, nil
}

// ProtoHeader represents the header of each protocol message.
// It will always be 8 bytes.
// Fields order:
// - Version 1 byte
// - Type 1 byte
// - Size 6 bytes
type ProtoHeader struct {
	Size    int
	Version int8
	Type    int8
}

// AerospikeMessage represents aerospike message type 3
// Fields order:
// - HeaderSize 1 byte
// - Info1 1 byte
// - Info2 1 byte
// - Info3 1 byte
// - ResultCode 1 byte
// - Generation 4 byte
// - RecordTTL 4 byte
// - TransactionTTL 4 byte
// - NumFields 2 byte
// - NumOps 2 byte
// - Fields
// - Ops (Is not parsed in the current version as we don't need them)
type AerospikeMessage struct {
	// Raw original message.
	Raw            []byte
	Fields         []*Field
	Generation     int
	RecordTTL      int
	TransactionTTL int
	NumFields      int16
	NumOps         int16
	// HeaderSize must always be 22 for proto version 2.
	HeaderSize int8
	Info1      int8
	Info2      int8
	Info3      int8
	ResultCode int8
}

// Field represents field of AerospikeMessage
// Fields order:
// - Size 4 bytes
// - Type 1 bytes
// - Data (Size - 1) bytes
type Field struct {
	Data []byte
	Size int
	Type int8
}

// ParseAerospikeMessage receives message body, without ProtoHeader.
// Returns *AerospikeMessage with parsed data.
func ParseAerospikeMessage(message []byte) (*AerospikeMessage, error) {
	if len(message) < LenMessageHeader {
		return nil, fmt.Errorf("message too short")
	}

	aMsg := &AerospikeMessage{
		Raw:        message,
		HeaderSize: fieldToInt8(message[:1]),
		Info1:      fieldToInt8(message[1:2]),
		Info2:      fieldToInt8(message[2:3]),
		Info3:      fieldToInt8(message[3:4]),
		// We skip bytes 4:5 as they are unused according to documentation.
		ResultCode:     fieldToInt8(message[5:6]),
		Generation:     fieldToInt(message[6:10]),
		RecordTTL:      fieldToInt(message[10:14]),
		TransactionTTL: fieldToInt(message[14:18]),
		NumFields:      fieldToInt16(message[18:20]),
		NumOps:         fieldToInt16(message[20:22]),
	}

	aMsg.Fields = ParseFields(message[LenMessageHeader:], aMsg.NumFields)

	return aMsg, nil
}

// ParseFields receives data part of a message, that is placed after MessageHeader - 22 bytes.
// And the number of fields that are encoded in this part of the message.
func ParseFields(message []byte, numFields int16) []*Field {
	if len(message) == 0 || numFields == 0 {
		return nil
	}

	result := make([]*Field, numFields)
	start := 0

	for i := range result {
		field, cursor := ParseField(message[start:])
		result[i] = field
		start += cursor
	}
	// If in future you'll need to parse operations,
	// you should return the last cursor to parse operations.

	return result
}

// ParseField parses one field, and returns *Field and cursor position.
func ParseField(message []byte) (field *Field, end int) {
	if len(message) < 5 {
		// Minimum required bytes
		return nil, 0
	}
	// As data size is (DataSize-1) bytes.
	// We start offset from 4, not 5
	const offset = 4

	// Size 4 bytes.
	size := fieldToInt(message[:4])
	// End of data block.
	end = offset + size

	field = &Field{
		Size: size,
		// Type 1 byte.
		Type: fieldToInt8(message[4:5]),
		Data: message[5:end],
	}

	return field, end
}

// NewPayload creates payload from received message.
func NewPayload(body []byte) []byte {
	bLen := len(body)

	msg := make([]byte, 0, bLen+LenProtoHeader)
	// version 1 byte
	msg = append(msg, intToField(ProtoVersion, 1)...)
	// type 1 byte
	msg = append(msg, intToField(ProtoTypeMessage, 1)...)
	// Len 6 byte
	msg = append(msg, intToField(bLen, 6)...)
	// Success message
	msg = append(msg, body...)

	return msg
}

// ResetXDRBit nullify xdr bit from Info1 field of AerospikeMessage.
// Receives body without a header.
func ResetXDRBit(message []byte) []byte {
	message[1] = 0
	return message
}

// NewAckMessage returns new acknowledge message.
func NewAckMessage(code int) []byte {
	msg := make([]byte, 0, LenProtoHeader+LenMessageHeader)
	// version 1 byte
	msg = append(msg, intToField(ProtoVersion, 1)...)
	// type 1 byte
	msg = append(msg, intToField(ProtoTypeMessage, 1)...)
	// Len 6 byte
	msg = append(msg, intToField(LenMessageHeader, 6)...)
	// Success message
	msg = append(msg, intToField(code, 22)...)

	return msg
}

// readBytes reads length number of bytes from conn.
func readBytes(conn net.Conn, length int) ([]byte, error) {
	buffer := make([]byte, length)
	total := 0

	for total < length {
		n, err := conn.Read(buffer[total:])
		if err != nil {
			return nil, err
		}

		total += n
	}

	return buffer, nil
}

func fieldToInt(header []byte) int {
	var num int
	for i := 0; i < len(header); i++ {
		num = (num << 8) | int(header[i])
	}

	return num
}

func fieldToInt8(header []byte) int8 {
	if len(header) == 0 {
		return 0
	}

	return int8(header[0])
}

func fieldToInt16(header []byte) int16 {
	var num int16
	for i := 0; i < len(header); i++ {
		num = (num << 8) | int16(header[i])
	}

	return num
}

func intToField(num, size int) []byte {
	field := make([]byte, size)
	for i := size - 1; i >= 0; i-- {
		field[i] = byte(num & 0xFF)
		num >>= 8
	}

	return field
}

const (
	FieldTypeNamespace = 0
	FieldTypeSet       = 1
	FieldTypeUserKey   = 2
	FieldTypeDigest    = 4

	UserKeyTypeInt    = 1
	UserKeyTypeString = 3
	UserKeyTypeBlob   = 4
)

// NewAerospikeKey returns new aerospike key from fields that we receive from
func NewAerospikeKey(fields []*Field) (*aerospike.Key, error) {
	if len(fields) < 4 {
		return nil, fmt.Errorf("not enough fields data")
	}

	var (
		namespace, set string
		digest         []byte
		key            any
	)

	for i := range fields {
		switch fields[i].Type {
		case FieldTypeNamespace:
			namespace = string(fields[i].Data)
		case FieldTypeSet:
			set = string(fields[i].Data)
		case FieldTypeUserKey:
			typeByte := fieldToInt(fields[i].Data[:1])
			switch typeByte {
			case UserKeyTypeInt:
				key = fieldToInt(fields[i].Data[1:])
			case UserKeyTypeString:
				key = string(fields[i].Data[1:])
			case UserKeyTypeBlob:
				key = fields[i].Data[1:]
			default:
				return nil, fmt.Errorf("unsupported key type %d", typeByte)
			}
		case FieldTypeDigest:
			digest = fields[i].Data
		default:
			// Skip any other fields, as we don't need them for key.
			continue
		}
	}

	ak, err := aerospike.NewKeyWithDigest(namespace, set, key, digest)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike key with digest: %w", err)
	}

	return ak, nil
}
