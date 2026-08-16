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

const (
	boolTrueByte  byte = 'T'
	boolFalseByte byte = 'F'
	asbNewLine         = '\n'
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

	recordHeader     = []byte{'+'}
	recordHeaderType = []byte{'k'}
	headerTypeInt    = []byte{'I'}
	headerTypeFloat  = []byte{'D'}
	headerTypeString = []byte{'S'}
	headerTypeBytes  = []byte{'B'}
)
