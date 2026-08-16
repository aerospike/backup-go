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

import (
	"bytes"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

// Exported wrappers preserve the legacy API for parity tests.

func KeyToASB(k *a.Key, w *bytes.Buffer) (int, error) {
	return keyToASB(k, w)
}

func BinToASB(name string, compact bool, v any, w *bytes.Buffer) (int, error) {
	return binToASB(name, compact, v, w)
}

func BinsToASB(compact bool, bins a.BinMap, w *bytes.Buffer) (int, error) {
	return binsToASB(compact, bins, w)
}

func UserKeyToASB(userKey a.Value, w *bytes.Buffer) (int, error) {
	return userKeyToASB(userKey, w)
}

func WriteBinBool(name string, v bool, w *bytes.Buffer) (int, error) {
	return writeBinBool(name, v, w)
}

func WriteBinInt[T binTypesInt](name string, v T, w *bytes.Buffer) (int, error) {
	return writeBinInt(name, v, w)
}

func WriteBinFloat(name string, v float64, w *bytes.Buffer) (int, error) {
	return writeBinFloat(name, v, w)
}

func WriteBinString(name, v string, w *bytes.Buffer) (int, error) {
	return writeBinString(name, v, w)
}

func WriteBinBytes(name string, compact bool, v []byte, w *bytes.Buffer) (int, error) {
	return writeBinBytes(name, compact, v, w)
}

func WriteBinHLL(name string, compact bool, v a.HLLValue, w *bytes.Buffer) (int, error) {
	return writeBinHLL(name, compact, v, w)
}

func WriteBinGeoJSON(name string, v a.GeoJSONValue, w *bytes.Buffer) (int, error) {
	return writeBinGeoJSON(name, v, w)
}

func WriteBinNil(name string, w *bytes.Buffer) (int, error) {
	return writeBinNil(name, w)
}

func WriteRecordNamespace(namespace string, w *bytes.Buffer) (int, error) {
	return writeRecordNamespace(namespace, w)
}

func WriteRecordDigest(digest []byte, w *bytes.Buffer) (int, error) {
	return writeRecordDigest(digest, w)
}

func WriteRecordSet(setName string, w *bytes.Buffer) (int, error) {
	return writeRecordSet(setName, w)
}

func WriteRecordHeaderGeneration(generation uint32, w *bytes.Buffer) (int, error) {
	return writeRecordHeaderGeneration(generation, w)
}

func WriteRecordHeaderExpiration(expiration int64, w *bytes.Buffer) (int, error) {
	return writeRecordHeaderExpiration(expiration, w)
}

func WriteRecordHeaderBinCount(binCount int, w *bytes.Buffer) (int, error) {
	return writeRecordHeaderBinCount(binCount, w)
}

func WriteUserKeyInt[T UserKeyTypesInt](v T, w *bytes.Buffer) (int, error) {
	return writeUserKeyInt(v, w)
}

func WriteUserKeyFloat(v float64, w *bytes.Buffer) (int, error) {
	return writeUserKeyFloat(v, w)
}

func WriteUserKeyString(v string, w *bytes.Buffer) (int, error) {
	return writeUserKeyString(v, w)
}

func WriteUserKeyBytes(v []byte, w *bytes.Buffer) (int, error) {
	return writeUserKeyBytes(v, w)
}

func BlobBinToASB(val []byte, bytesType byte, name string) []byte {
	return blobBinToASB(val, bytesType, name)
}

func BoolToASB(b bool) []byte {
	return boolToASB(b)
}

func WriteRawListBin(cdt *a.RawBlobValue, name string, compact bool, w *bytes.Buffer) (int, error) {
	return writeRawListBin(cdt, name, compact, w)
}

func WriteRawMapBin(cdt *a.RawBlobValue, name string, compact bool, w *bytes.Buffer) (int, error) {
	return writeRawMapBin(cdt, name, compact, w)
}

func WriteRawBlobBin(cdt *a.RawBlobValue, name string, compact bool, w *bytes.Buffer) (int, error) {
	return writeRawBlobBin(cdt, name, compact, w)
}

// Record is a type alias used by legacy tests.
type Record = models.Record
