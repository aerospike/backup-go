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

package legacy_encoder

import "github.com/aerospike/backup-go/io/encoding/asb/internal/lineprefix"

var (
	space                     = lineprefix.Space
	newLine                   = lineprefix.NewLine
	binBoolTypePrefix         = lineprefix.BinBoolTypePrefix
	binIntTypePrefix          = lineprefix.BinIntTypePrefix
	binFloatTypePrefix        = lineprefix.BinFloatTypePrefix
	binStringTypePrefix       = lineprefix.BinStringTypePrefix
	binBytesTypePrefix        = lineprefix.BinBytesTypePrefix
	binBytesTypeCompactPrefix = lineprefix.BinBytesTypeCompactPrefix
	binHLLTypePrefix          = lineprefix.BinHLLTypePrefix
	binHLLTypeCompactPrefix   = lineprefix.BinHLLTypeCompactPrefix
	binGeoJSONTypePrefix      = lineprefix.BinGeoJSONTypePrefix
	binNilTypePrefix          = lineprefix.BinNilTypePrefix
	binMapTypePrefix          = lineprefix.BinMapTypePrefix
	binMapTypeCompactPrefix   = lineprefix.BinMapTypeCompactPrefix
	binListTypePrefix         = lineprefix.BinListTypePrefix
	binListTypeCompactPrefix  = lineprefix.BinListTypeCompactPrefix
	trueBytes                 = lineprefix.TrueBytes
	falseBytes                = lineprefix.FalseBytes
	namespacePrefix           = lineprefix.NamespacePrefix
	setPrefix                 = lineprefix.SetPrefix
	digestPrefix              = lineprefix.DigestPrefix
	headerGeneration          = lineprefix.HeaderGeneration
	headerExpiration          = lineprefix.HeaderExpiration
	headerBinCount            = lineprefix.HeaderBinCount
	recordHeader              = lineprefix.RecordHeader
	recordHeaderType          = lineprefix.RecordHeaderType
	headerTypeInt             = lineprefix.HeaderTypeInt
	headerTypeFloat           = lineprefix.HeaderTypeFloat
	headerTypeString          = lineprefix.HeaderTypeString
	headerTypeBytes           = lineprefix.HeaderTypeBytes
)
