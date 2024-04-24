// Copyright 2024-2024 Aerospike, Inc.
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

package models

import (
	"math"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Records ****

const (
	// VoidTimeNeverExpire is used when a record should never expire
	VoidTimeNeverExpire int64 = 0
	// ExpirationNever is the Aerospike server's special TTL value for records that should never expire
	ExpirationNever uint32 = math.MaxUint32
)

type Record struct {
	*a.Record

	// VoidTime is the time in seconds since the citrusleaf epoch when the record will expire.
	VoidTime int64
}

// **** SIndexes ****

type SIPathBinType byte

const (
	InvalidSIDataType     SIPathBinType = 0
	NumericSIDataType     SIPathBinType = 'N'
	StringSIDataType      SIPathBinType = 'S'
	GEO2DSphereSIDataType SIPathBinType = 'G'
	BlobSIDataType        SIPathBinType = 'B'
)

type SIndexType byte

const (
	InvalidSIndex     SIndexType = 0
	BinSIndex         SIndexType = 'N'
	ListElementSIndex SIndexType = 'L'
	MapKeySIndex      SIndexType = 'K'
	MapValueSIndex    SIndexType = 'V'
)

type SIndexPath struct {
	BinName    string
	B64Context string
	BinType    SIPathBinType
}

type SIndex struct {
	Namespace string
	Set       string
	Name      string
	Path      SIndexPath
	IndexType SIndexType
}

// **** UDFs ****

type UDFType byte

const (
	UDFTypeLUA UDFType = 'L'
)

type UDF struct {
	Name    string
	Content []byte
	UDFType UDFType
}

// **** Token ****

type TokenType uint8

const (
	TokenTypeInvalid TokenType = iota
	TokenTypeRecord
	TokenTypeSIndex
	TokenTypeUDF
)

// Token encompasses the other data models
// fields should be accessed based on the tokenType
type Token struct {
	SIndex *SIndex
	UDF    *UDF
	Record Record
	Type   TokenType
}

// NewRecordToken creates a new token with the given record
func NewRecordToken(r Record) *Token {
	return &Token{
		Record: r,
		Type:   TokenTypeRecord,
	}
}

// NewSIndexToken creates a new token with the given secondary index
func NewSIndexToken(s *SIndex) *Token {
	return &Token{
		SIndex: s,
		Type:   TokenTypeSIndex,
	}
}

// NewUDFToken creates a new token with the given UDF
func NewUDFToken(u *UDF) *Token {
	return &Token{
		UDF:  u,
		Type: TokenTypeUDF,
	}
}
