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

package asb

import "math"

// section names
const (
	sectionGlobal = "global"
	sectionRecord = "records"
)

// asb section markers
const (
	markerGlobalSection   byte = '*'
	markerMetadataSection byte = '#'
	markerRecordHeader    byte = '+'
	markerRecordBins      byte = '-'
)

// line names
const (
	lineTypeNamespace  = "namespace"
	lineTypeUDF        = "UDF"
	lineTypeSindex     = "sindex"
	lineTypeRecordBins = "record bins"
	lineTypeKey        = "key"
	lineTypeFirst      = "first"
)

// global line types
const (
	globalTypeSIndex           byte = 'i'
	globalTypeUDF              byte = 'u'
	globalTypeSIndexExpression byte = 'e'
)

// key types
const (
	keyTypeInt          byte = 'I'
	keyTypeFloat        byte = 'D'
	keyTypeString       byte = 'S'
	keyTypeStringBase64 byte = 'X'
	keyTypeBytes        byte = 'B'
)

// record header types
const (
	recordHeaderTypeKey        byte = 'k'
	recordHeaderTypeNamespace  byte = 'n'
	recordHeaderTypeDigest     byte = 'd'
	recordHeaderTypeSet        byte = 's'
	recordHeaderTypeGen        byte = 'g'
	recordHeaderTypeExpiration byte = 't'
	recordHeaderTypeBinCount   byte = 'b'
)

// bin types
const (
	binTypeNil          byte = 'N'
	binTypeBool         byte = 'Z'
	binTypeInt          byte = 'I'
	binTypeFloat        byte = 'D'
	binTypeString       byte = 'S'
	binTypeBytes        byte = 'B'
	binTypeBytesJava    byte = 'J'
	binTypeBytesCSharp  byte = 'C'
	binTypeBytesPython  byte = 'P'
	binTypeBytesRuby    byte = 'R'
	binTypeBytesPHP     byte = 'H'
	binTypeBytesErlang  byte = 'E'
	binTypeBytesHLL     byte = 'Y'
	binTypeBytesMap     byte = 'M'
	binTypeBytesList    byte = 'L'
	binTypeLDT          byte = 'U'
	binTypeStringBase64 byte = 'X'
	binTypeGeoJSON      byte = 'G'
)

// sindex types
const (
	sindexTypeBin    byte = 'N'
	sindexTypeList   byte = 'L'
	sindexTypeMapKey byte = 'K'
	sindexTypeMapVal byte = 'V'
)

// sindex bin types
const (
	sindexBinTypeString  byte = 'S'
	sindexBinTypeNumeric byte = 'N'
	sindexBinTypeGEO2D   byte = 'G'
	sindexBinTypeBlob    byte = 'B'
)

// literal asb tokens
const (
	tokenNamespace  = "namespace"
	tokenFirstFile  = "first-file"
	tokenASBVersion = "Version"
)

// value bounds
const (
	maxTokenSize  = 1000
	maxGeneration = math.MaxUint16
	maxBinCount   = math.MaxUint16
)

// asb boolean encoding
const (
	boolTrueByte  byte = 'T'
	boolFalseByte byte = 'F'
)

// escape character
const (
	asbEscape  = '\\'
	asbNewLine = '\n'
)

var (
	space                     = []byte(" ")
	newLine                   = []byte("\n")
	binBoolTypePrefix         = []byte("- Z ")
	binIntTypePrefix          = []byte("- I ")
	binFloatTypePrefix        = []byte("- D ")
	binStringTypePrefix       = []byte("- S ")
	binBytesTypePrefix        = []byte("- B ")
	binBytesTypeCompactPrefix = []byte("- B! ")
	binHLLTypePrefix          = []byte("- Y ")
	binHLLTypeCompactPrefix   = []byte("- Y! ")
	binGeoJSONTypePrefix      = []byte("- G ")
	binNilTypePrefix          = []byte("- N ")
	binMapTypePrefix          = []byte("- M ")
	binMapTypeCompactPrefix   = []byte("- M! ")
	binListTypePrefix         = []byte("- L ")
	binListTypeCompactPrefix  = []byte("- L! ")
	trueBytes                 = []byte{boolTrueByte}
	falseBytes                = []byte{boolFalseByte}
	namespacePrefix           = []byte("+ n ")
	setPrefix                 = []byte("+ s ")
	digestPrefix              = []byte("+ d ")
	headerGeneration          = []byte("+ g ")
	headerExpiration          = []byte("+ t ")
	headerBinCount            = []byte("+ b ")

	recordHeader     = []byte{markerRecordHeader}
	recordHeaderType = []byte{recordHeaderTypeKey}
	headerTypeInt    = []byte{keyTypeInt}
	headerTypeFloat  = []byte{keyTypeFloat}
	headerTypeString = []byte{keyTypeString}
	headerTypeBytes  = []byte{keyTypeBytes}

	globalSection          = []byte{markerGlobalSection}
	globalSIndex           = []byte{globalTypeSIndex}
	globalSIndexExpression = []byte{globalTypeSIndexExpression}
	globalUDF              = []byte{globalTypeUDF}

	tokenVersion    = []byte(tokenASBVersion)
	tokenFirst      = []byte(tokenFirstFile)
	namespaceToken  = []byte(tokenNamespace)
	metadataSection = []byte{markerMetadataSection}
)
