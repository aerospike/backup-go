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
	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Records ****

type Record = a.Record

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
	BinType    SIPathBinType
	B64Context string
}

type SIndex struct {
	Namespace string
	Set       string
	Name      string
	IndexType SIndexType
	Path      SIndexPath
}

// **** UDFs ****

type UDFType byte

const (
	LUAUDFType UDFType = 'L'
)

type UDF struct {
	UDFType UDFType
	Name    string
	Content []byte
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
	Record *Record
	SIndex *SIndex
	UDF    *UDF
	Type   TokenType
}

// NewRecordToken creates a new token with the given record
func NewRecordToken(r *Record) *Token {
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
