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

package asb

import "math"

// section names
const (
	sectionUndefined = ""
	sectionHeader    = "header"
	sectionMetadata  = "meta-data"
	sectionGlobal    = "global"
	sectionRecord    = "records"
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
	lineTypeUndefined    = ""
	lineTypeVersion      = "version"
	lineTypeNamespace    = "namespace"
	lineTypeUDF          = "UDF"
	lineTypeSindex       = "sindex"
	lineTypeRecordHeader = "record header"
	lineTypeRecordBins   = "record bins"
	lineTypeBin          = "bin"
	lineTypeKey          = "key"
	lineTypeDigest       = "digest"
	lineTypesSet         = "set"
	lineTypeGen          = "generation"
	lineTypeExpiration   = "expiration"
	lineTypeBinCount     = "bin count"
	lineTypeFirst        = "first"
)

// literal asb tokens
const (
	tokenNamespace  = "namespace"
	tokenFirstFile  = "first-file"
	toeknASBVersion = "Version"
)

// value bounds
const (
	maxNamespaceLength = 31
	maxTokenSize       = 1000
	maxGeneration      = math.MaxUint16
	maxBinCount        = math.MaxUint16
)

// asb boolean encoding
const (
	boolTrueByte  byte = 'T'
	boolFalseByte byte = 'F'
)

// escape character
const (
	asbEscape = '\\'
)

// misc constants
const (
	// citrusLeafEpoch is the number of seconds between the Unix epoch and the Aerospike epoch
	citrusLeafEpoch = 1262304000 // pulled from C client cf_clock.h
	// ASBFormatVersion is the current version of the ASB encoding format
	ASBFormatVersion = "3.1"
)
