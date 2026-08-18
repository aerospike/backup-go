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

import (
	"flag"
	"fmt"
	"math"
	"strconv"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const propertyTestChecks = 1000

func init() {
	// rapid defaults to 100 checks; raise for stronger coverage in CI and local runs.
	_ = flag.Set("rapid.checks", strconv.Itoa(propertyTestChecks))
}

func TestEncodeDecodeRecordRoundTripProperty(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		compact := rapid.Bool().Draw(t, "compact")
		want := drawRecord(t)

		got, err := roundTripSingleRecord(compact, want)
		require.NoError(t, err)
		require.Equal(t, normalizeRecord(want), got)
	})
}

func roundTripSingleRecord(compact bool, record *models.Record) (*models.Record, error) {
	records, err := roundTripEncodeDecode(compact, models.NewRecordToken(record, 0, nil))
	if err != nil {
		return nil, err
	}

	if len(records) != 1 {
		return nil, fmt.Errorf("record count mismatch: got %d want 1", len(records))
	}

	return records[0], nil
}

func drawRecord(t *rapid.T) *models.Record {
	t.Helper()

	namespace := drawASBString(t, "namespace")
	if namespace == "" {
		namespace = "ns"
	}

	set := drawASBString(t, "set")
	userKey := drawUserKey(t)

	key, keyErr := a.NewKey(namespace, set, userKey)
	if keyErr != nil {
		t.Fatalf("failed to build key: %v", keyErr)
	}

	return &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       drawBins(t),
			Generation: rapid.Uint32Range(0, uint32(maxGeneration)).Draw(t, "generation"),
		},
		VoidTime: rapid.Int64Min(0).Draw(t, "voidTime"),
	}
}

func drawUserKey(t *rapid.T) any {
	t.Helper()

	switch rapid.IntRange(0, 2).Draw(t, "userKeyKind") {
	case 0:
		return drawASBString(t, "userKeyString")
	case 1:
		return rapid.Int64().Draw(t, "userKeyInt")
	default:
		return drawBytes(t, "userKeyBytes", 64)
	}
}

func drawBins(t *rapid.T) a.BinMap {
	t.Helper()

	count := rapid.IntRange(0, 24).Draw(t, "binCount")
	bins := make(a.BinMap, count)

	for i := range count {
		name := fmt.Sprintf("%s_%d", drawASBString(t, "binName"), i)
		bins[name] = drawBinValue(t)
	}

	return bins
}

func drawBinValue(t *rapid.T) any {
	t.Helper()

	switch rapid.IntRange(0, 13).Draw(t, "binKind") {
	case 0:
		return nil
	case 1:
		return rapid.Bool().Draw(t, "boolBin")
	case 2:
		return rapid.Int64().Draw(t, "int64Bin")
	case 3:
		return rapid.Int32().Draw(t, "int32Bin")
	case 4:
		return rapid.Int16().Draw(t, "int16Bin")
	case 5:
		return rapid.Int8().Draw(t, "int8Bin")
	case 6:
		return int(rapid.Int32().Draw(t, "intBin"))
	case 7:
		return drawFiniteFloat(t, "floatBin")
	case 8:
		return drawASBString(t, "stringBin")
	case 9:
		return drawBytes(t, "bytesBin", 512)
	case 10:
		return a.HLLValue(drawBytes(t, "hllBin", 128))
	case 11:
		return a.GeoJSONValue(drawGeoJSON(t))
	case 12:
		return &a.RawBlobValue{
			ParticleType: particleType.MAP,
			Data:         drawBytes(t, "mapBin", 256),
		}
	default:
		return &a.RawBlobValue{
			ParticleType: particleType.LIST,
			Data:         drawBytes(t, "listBin", 256),
		}
	}
}

func drawASBString(t *rapid.T, label string) string {
	t.Helper()

	return rapid.OneOf(
		rapid.StringMatching(`[A-Za-z0-9_\-]+`),
		rapid.StringMatching(`[A-Za-z0-9_\-]+ [A-Za-z0-9_\-]+`),
		rapid.String(),
	).Draw(t, label)
}

func drawBytes(t *rapid.T, label string, maxLen int) []byte {
	t.Helper()

	return rapid.SliceOfN(rapid.Byte(), 0, maxLen).Draw(t, label)
}

func drawFiniteFloat(t *rapid.T, label string) float64 {
	t.Helper()

	return rapid.Float64().Filter(func(value float64) bool {
		return !math.IsNaN(value) && !math.IsInf(value, 0)
	}).Draw(t, label)
}

func drawGeoJSON(t *rapid.T) string {
	t.Helper()

	lon := drawFiniteFloat(t, "geoLon")
	lat := drawFiniteFloat(t, "geoLat")

	return fmt.Sprintf(`{"type":"Point","coordinates":[%g,%g]}`, lon, lat)
}
