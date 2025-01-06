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

package models

import (
	"math"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	// VoidTimeNeverExpire is used when a record should never expire.
	VoidTimeNeverExpire int64 = 0
	// ExpirationNever is the Aerospike server's special TTL value for records
	// that should never expire.
	ExpirationNever uint32 = math.MaxUint32
)

type Record struct {
	*a.Record

	// VoidTime is the time in seconds since the citrusleaf epoch when the
	// record will expire.
	VoidTime int64
}

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

type UDFType byte

const (
	UDFTypeLUA UDFType = 'L'
)

type UDF struct {
	Name    string
	Content []byte
	UDFType UDFType
}
