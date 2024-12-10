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

// Read reads messages fom connection and returns raw bytes with proto body.
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

	bodyBytes, err := readBytes(p.conn, int(size))
	if err != nil {
		return nil, fmt.Errorf("failed to read proto body bytes: %w", err)
	}

	return bodyBytes, nil
}

// ProtoHeader represents the header of each protocol message.
// It will always be 8 bytes.
type ProtoHeader struct {
	// Version 1 byte.
	Version uint64
	// Type 1 byte.
	Type uint64
	// Size 6 bytes.
	Size uint64
}

// AerospikeMessage represents aerospike message type 3
type AerospikeMessage struct {
	// Original message.
	Raw []byte
	// Must be always 22 for proto version 2.
	HeaderSize     uint64
	Info1          uint64
	Info2          uint64
	Info3          uint64
	ResultCode     uint64
	Generation     uint64
	RecordTTL      uint64
	TransactionTTL uint64
	NumFields      uint64
	NumOps         uint64
	Fields         []*Field
	// There will be operations. But we don't need to parse them.
}

// Field represents field of AerospikeMessage
type Field struct {
	Size uint64
	Type uint64
	Data []byte // or interface
}

// ParseAerospikeMessage receives message body, without ProtoHeader.
// Returns *AerospikeMessage with parsed data.
func ParseAerospikeMessage(message []byte) (*AerospikeMessage, error) {
	if len(message) < LenMessageHeader {
		return nil, fmt.Errorf("message too short")
	}

	aMsg := &AerospikeMessage{
		Raw:        message,
		HeaderSize: fieldToInt(message[:1]),
		Info1:      fieldToInt(message[1:2]),
		Info2:      fieldToInt(message[2:3]),
		Info3:      fieldToInt(message[3:4]),
		// We skip bytes 4:5 as they are unused according to documentation.
		ResultCode:     fieldToInt(message[5:6]),
		Generation:     fieldToInt(message[6:10]),
		RecordTTL:      fieldToInt(message[10:14]),
		TransactionTTL: fieldToInt(message[14:18]),
		NumFields:      fieldToInt(message[18:20]),
		NumOps:         fieldToInt(message[20:22]),
	}

	aMsg.Fields = ParseFields(message[LenMessageHeader:], aMsg.NumFields)

	return aMsg, nil
}

// ParseFields receives data part of a message, that is placed after MessageHeader - 22 bytes.
// And the number of fields that are encoded in this part of the message.
func ParseFields(message []byte, numFields uint64) []*Field {
	if len(message) == 0 || numFields == 0 {
		return nil
	}

	result := make([]*Field, numFields)
	start := uint64(0)

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
func ParseField(message []byte) (field *Field, end uint64) {
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
		Type: fieldToInt(message[4:5]),
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
	msg = append(msg, intToField(uint64(bLen), 6)...)
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
func NewAckMessage(code uint64) []byte {
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

func fieldToInt(header []byte) uint64 {
	var num uint64
	for i := 0; i < len(header); i++ {
		num = (num << 8) | uint64(header[i])
	}

	return num
}

func intToField(num uint64, size int) []byte {
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
