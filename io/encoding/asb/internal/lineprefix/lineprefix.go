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

// Package lineprefix holds shared ASB line prefix bytes used by the production
// and legacy record encoders.
package lineprefix

const (
	BoolTrueByte  byte = 'T'
	BoolFalseByte byte = 'F'
)

var (
	Space   = []byte(" ")
	NewLine = []byte("\n")

	BinBoolTypePrefix         = []byte("- Z ")
	BinIntTypePrefix          = []byte("- I ")
	BinFloatTypePrefix        = []byte("- D ")
	BinStringTypePrefix       = []byte("- S ")
	BinBytesTypePrefix        = []byte("- B ")
	BinBytesTypeCompactPrefix = []byte("- B! ")
	BinHLLTypePrefix          = []byte("- Y ")
	BinHLLTypeCompactPrefix   = []byte("- Y! ")
	BinGeoJSONTypePrefix      = []byte("- G ")
	BinNilTypePrefix          = []byte("- N ")
	BinMapTypePrefix          = []byte("- M ")
	BinMapTypeCompactPrefix   = []byte("- M! ")
	BinListTypePrefix         = []byte("- L ")
	BinListTypeCompactPrefix  = []byte("- L! ")

	TrueBytes  = []byte{BoolTrueByte}
	FalseBytes = []byte{BoolFalseByte}

	NamespacePrefix     = []byte("+ n ")
	SetPrefix           = []byte("+ s ")
	DigestPrefix        = []byte("+ d ")
	HeaderGeneration    = []byte("+ g ")
	HeaderExpiration    = []byte("+ t ")
	HeaderBinCount      = []byte("+ b ")
	UserKeyIntPrefix    = []byte("+ k I ")
	UserKeyFloatPrefix  = []byte("+ k D ")
	UserKeyStringPrefix = []byte("+ k S ")
	UserKeyBytesPrefix  = []byte("+ k B ")

	RecordHeader     = []byte{'+'}
	RecordHeaderType = []byte{'k'}
	HeaderTypeInt    = []byte{'I'}
	HeaderTypeFloat  = []byte{'D'}
	HeaderTypeString = []byte{'S'}
	HeaderTypeBytes  = []byte{'B'}

	SIndexSizeOne = []byte("1")
)
