// Copyright 2024-2026 Aerospike, Inc.
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

//go:build integration

package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/aerospike/backup-go/tests"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/require"
)

const (
	// got this from writing and reading back a.HLLAddOp(hllpol, "hll", []a.Value{a.NewIntegerValue(1)}, 4, 12)
	hllValue = "\x00\x04\f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x7f\x84\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)

const (
	testASLoginPassword = "admin"
	testASNamespace     = "test"
	testASHost          = "127.0.0.1"
	testASPort          = 3000
	testTimeout         = 60 * time.Second
)

// Record size buckets. Aerospike caps a record, metadata included, at 8 MiB;
// max-record-size is the namespace knob that enforces it and since server
// 7.1.0 it defaults to 1 MiB, so the 7 MiB bucket needs it raised first.
const (
	dtMaxRecordSize = 8 << 20

	dtSizeScalar = 0         // fixed-width types: the size bucket does not apply
	dtSizeSmall  = 1536      // 1.5 KiB
	dtSizeMedium = 100 << 10 // 100 KiB
	dtSizeLarge  = 1 << 20   // 1 MiB
	dtSizeHuge   = 7 << 20   // 7 MiB
)

// Records per bucket. The small buckets carry the full sample; the large ones
// are trimmed because 100 records of 7 MiB is 700 MiB per set, which neither
// the namespace nor the package test timeout can absorb. The point of the test
// is type coverage, and that is already reached with a handful of records.
const (
	dtRecordsDefault = 100
	dtRecordsLarge   = 20
	dtRecordsHuge    = 5
)

const (
	dtSetPrefix   = "testDT"
	dtTestTimeout = 3 * time.Minute

	// dtLargeRecordThreshold is the record size above which the test throttles
	// both the write path and the restore path. Multi-megabyte records pushed
	// through the default batch settings make the server answer with
	// DEVICE_OVERLOAD.
	dtLargeRecordThreshold = 128 << 10

	// dtInFlightBudget bounds how many bytes one restore batch may carry.
	dtInFlightBudget = 8 << 20

	// dtLargeMaxAsyncBatches limits concurrent restore batches for large records.
	dtLargeMaxAsyncBatches = 2

	dtMaxRetries          = 5
	dtSleepBetweenRetries = 200 * time.Millisecond
)

// Type names, used to build set and subtest names.
const (
	dtTypeInt     = "Int"
	dtTypeFloat   = "Float"
	dtTypeBool    = "Bool"
	dtTypeString  = "String"
	dtTypeBlob    = "Blob"
	dtTypeList    = "List"
	dtTypeMap     = "Map"
	dtTypeGeoJSON = "GeoJSON"
	dtTypeHLL     = "HLL"
)

// Bin names.
const (
	dtBinInt     = "IntBin"
	dtBinFloat   = "FloatBin"
	dtBinBool    = "BoolBin"
	dtBinString  = "StringBin"
	dtBinBlob    = "BlobBin"
	dtBinList    = "ListBin"
	dtBinMap     = "MapBin"
	dtBinGeoJSON = "GeoJSONBin"
	dtBinHLL     = "HLLBin"
)

// Map keys used by the nested structures.
const (
	dtMapKeyIntValue = 1 // integer map key, Aerospike supports those next to strings
	dtMapKeyInt      = "int"
	dtMapKeyFloat    = "float"
	dtMapKeyString   = "string"
	dtMapKeyBool     = "bool"
	dtMapKeyNil      = "nil"
	dtMapKeyList     = "list"
	dtMapKeyNested   = "nested"
	dtMapKeyPayload  = "payload"
)

// Info command building blocks.
const (
	dtMaxRecordSizeParam = "max-record-size"
	dtInfoResponseOK     = "ok"
)

// Payload generation.
const (
	// dtSeed keeps every generated payload reproducible across runs.
	dtSeed uint64 = 0x5DEECE66D

	// dtStructureOverhead is a deliberate overestimate of the fixed part of a
	// generated list or map, subtracted from the target size before the filler
	// is produced. Sizes are therefore exact for strings and blobs and
	// approximate for the structured types.
	dtStructureOverhead = 512
	dtMinPayloadSize    = 16
	dtNestedSize        = 64
	dtPadByte           = 'x'

	dtUserKeyKinds  = 3
	dtUserKeyPrefix = "dtKey"
)

// GeoJSON generation. The polygon is a circle approximated with as many
// vertices as the target size allows.
const (
	dtGeoPrefix       = `{"type":"Polygon","coordinates":[[`
	dtGeoSuffix       = `]]}`
	dtGeoVertexLen    = 21 // approximate length of one "[lon,lat]," chunk
	dtGeoMinVertices  = 8
	dtGeoPrecision    = 6
	dtGeoBaseRadius   = 1.0
	dtGeoRadiusStepPD = 1000.0 // per-record radius step, keeps every polygon distinct
)

// HLL particle geometry. The particle occupies 2^indexBits * (6 + minHashBits)
// bits, so these two shapes land on 1536 B and 102400 B respectively. There is
// no configuration that reaches 1 MiB, which is why HLL has no large buckets.
const (
	dtHLLSmallIndexBits    = 11
	dtHLLMediumIndexBits   = 14
	dtHLLMediumMinHashBits = 44
	// dtHLLNoMinHash disables the minhash part of the particle.
	dtHLLNoMinHash = -1
)

// dtIntValues cycles through the edges of the integer particle.
var dtIntValues = []int{0, 1, -1, math.MaxInt32, math.MinInt32, math.MaxInt64, math.MinInt64}

// dtFloatValues cycles through the edges of the double particle. NaN and the
// infinities are left out on purpose: they are not comparable by value.
var dtFloatValues = []float64{
	0,
	1.1,
	-1.1,
	math.Pi,
	math.MaxFloat64,
	-math.MaxFloat64,
	math.SmallestNonzeroFloat64,
}

// dtStringAlphabet mixes ASCII, multi-byte UTF-8 and the characters the ASB
// encoder has to escape, so the string bucket also exercises escaping.
var dtStringAlphabet = []rune("abcXYZ019 \n\r\t\\\"'привет日本語😀")

// dtSizeClass is one record-size bucket of the type matrix.
type dtSizeClass struct {
	name    string
	size    int
	records int
}

var (
	dtClassScalar = dtSizeClass{name: "scalar", size: dtSizeScalar, records: dtRecordsDefault}
	dtClassSmall  = dtSizeClass{name: "1536b", size: dtSizeSmall, records: dtRecordsDefault}
	dtClassMedium = dtSizeClass{name: "100kb", size: dtSizeMedium, records: dtRecordsDefault}
	dtClassLarge  = dtSizeClass{name: "1mb", size: dtSizeLarge, records: dtRecordsLarge}
	dtClassHuge   = dtSizeClass{name: "7mb", size: dtSizeHuge, records: dtRecordsHuge}
)

// dtWriter populates one test set and reports the bins the database is expected
// to hold, keyed by record digest.
type dtWriter interface {
	Write(client *a.Client, keys []*a.Key, size int) (map[digestT]a.BinMap, error)
}

// dtSpec describes one Aerospike data type under test.
type dtSpec struct {
	writer  dtWriter
	name    string
	classes []dtSizeClass
}

// testBins is a collection of all supported bin types
// useful for testing backup and restore
var testBins = a.BinMap{
	"IntBin":     1,
	"FloatBin":   1.1,
	"StringBin":  "string",
	"BoolBin":    true,
	"BlobBin":    []byte("bytes"),
	"GeoJSONBin": a.GeoJSONValue(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
	"HLLBin":     a.NewHLLValue([]byte(hllValue)),
	"MapBin": map[any]any{
		"IntBin":    1,
		"StringBin": "hi",
		"listBin":   []any{1, 2, 3},
		"mapBin":    map[any]any{1: 1},
	},
	"ListBin": []any{
		1,
		"string",
		[]byte("bytes"),
		map[any]any{1: 1},
		[]any{1, 2, 3},
	},
}

func testAerospikeClient() (*a.Client, error) {
	aeroClientPolicy := a.NewClientPolicy()
	aeroClientPolicy.User = testASLoginPassword
	aeroClientPolicy.Password = testASLoginPassword
	aeroClientPolicy.Timeout = testTimeout
	return a.NewClientWithPolicy(
		aeroClientPolicy,
		testASHost,
		testASPort,
	)
}

func testInfoClient(client *a.Client) (*asinfo.Client, error) {
	return asinfo.NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
}

func runBackupRestoreLocal(
	ctx context.Context,
	client *a.Client,
	directory string,
	backupConfig *ConfigBackup,
	restoreConfig *ConfigRestore,
) (*models.BackupStats, *models.RestoreStats, error) {
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create directory %s: %w", directory, err)
	}

	backupClient, err := NewClient(client)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	writer, err := local.NewWriter(
		ctx,
		options.WithRemoveFiles(),
		options.WithDir(directory),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create local writer: %w", err)
	}

	bh, err := backupClient.Backup(
		ctx,
		backupConfig,
		writer,
		nil,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to backup: %w", err)
	}

	err = bh.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to wait backup: %w", err)
	}

	err = client.Truncate(nil, testASNamespace, backupConfig.SetList[0], nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to truncate: %w", err)
	}

	time.Sleep(1 * time.Second)

	reader, err := local.NewReader(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithDir(directory),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create local reader: %w", err)
	}

	rh, err := backupClient.Restore(
		ctx,
		restoreConfig,
		reader,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to restore: %w", err)
	}

	err = rh.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to wait restore: %w", err)
	}

	return bh.GetStats(), rh.GetStats(), nil
}

// TestBackupRestoreAllDataTypes writes every Aerospike data type in every
// record-size bucket that makes sense for it, backs the set up, truncates it,
// restores it and asserts that the restored records match the pre-backup
// snapshot bit for bit, nested values included.
//
// The test is deliberately not parallel: it writes multi-megabyte records and
// running it next to the rest of the integration suite provokes DEVICE_OVERLOAD.
// Sets are processed one at a time and truncated afterwards for the same reason.
func TestBackupRestoreAllDataTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), dtTestTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)

	// t.Cleanup rather than defer: deferred calls run when the test body
	// returns, cleanups run after that, so a deferred Close would shut the
	// client down before the max-record-size rollback could reach the cluster.
	// Cleanups run LIFO, so registering Close first makes it run last.
	t.Cleanup(asClient.Close)
	t.Cleanup(dtRaiseMaxRecordSize(t, asClient))

	for _, spec := range dtSpecs() {
		for _, class := range spec.classes {
			t.Run(fmt.Sprintf("%s_%s", spec.name, class.name), func(t *testing.T) {
				dtRunCase(ctx, t, asClient, spec, class)
			})
		}
	}
}

// dtSpecs is the type matrix. Fixed-width types get a single bucket; only the
// container types can be grown to arbitrary sizes.
func dtSpecs() []dtSpec {
	scalable := []dtSizeClass{dtClassSmall, dtClassMedium, dtClassLarge, dtClassHuge}

	return []dtSpec{
		{
			name:    dtTypeInt,
			writer:  dtGenWriter{gen: dtGenInt},
			classes: []dtSizeClass{dtClassScalar},
		},
		{
			name:    dtTypeFloat,
			writer:  dtGenWriter{gen: dtGenFloat},
			classes: []dtSizeClass{dtClassScalar},
		},
		{
			name:    dtTypeBool,
			writer:  dtGenWriter{gen: dtGenBool},
			classes: []dtSizeClass{dtClassScalar},
		},
		{
			name:    dtTypeString,
			writer:  dtGenWriter{gen: dtGenString},
			classes: scalable,
		},
		{
			name:    dtTypeBlob,
			writer:  dtGenWriter{gen: dtGenBlob},
			classes: scalable,
		},
		{
			name:    dtTypeList,
			writer:  dtGenWriter{gen: dtGenList},
			classes: scalable,
		},
		{
			name:    dtTypeMap,
			writer:  dtGenWriter{gen: dtGenMap},
			classes: scalable,
		},
		{
			// A GeoJSON value is validated by the server on every write, so it
			// is kept small on purpose.
			name:    dtTypeGeoJSON,
			writer:  dtGenWriter{gen: dtGenGeoJSON},
			classes: []dtSizeClass{dtClassSmall},
		},
		{
			name:    dtTypeHLL,
			writer:  dtHLLWriter{indexBits: dtHLLSmallIndexBits, minHashBits: dtHLLNoMinHash},
			classes: []dtSizeClass{dtClassSmall},
		},
		{
			name:    dtTypeHLL,
			writer:  dtHLLWriter{indexBits: dtHLLMediumIndexBits, minHashBits: dtHLLMediumMinHashBits},
			classes: []dtSizeClass{dtClassMedium},
		},
	}
}

// dtRunCase runs the full write - backup - truncate - restore - compare cycle
// for a single (type, size) pair.
func dtRunCase(ctx context.Context, t *testing.T, client *a.Client, spec dtSpec, class dtSizeClass) {
	t.Helper()

	setName := dtSetName(spec.name, class.name)
	t.Cleanup(func() {
		// Leaving multi-megabyte records behind would skew the namespace for
		// every test that runs after this one.
		require.NoError(t, client.Truncate(nil, testASNamespace, setName, nil))
	})

	keys, err := dtGenKeys(setName, class.records)
	require.NoError(t, err)

	generated, err := spec.writer.Write(client, keys, class.size)
	require.NoError(t, err)
	require.Len(t, generated, class.records)

	// Snapshot of the set exactly as the database holds it before the backup.
	// This is the reference the restored records must match. For HLL the
	// generated bins were themselves read back from the database, so the check
	// against `generated` is a tautology there and a real assertion everywhere else.
	before, err := readAllRecords(client, testASNamespace, setName)
	require.NoError(t, err)
	require.Equal(t, class.records, before.Len())

	for _, key := range keys {
		record, ok := before.Get(string(key.Digest()))
		require.Truef(t, ok, "record was not written: %v", key)
		dtRequireBinsEqual(t, generated[string(key.Digest())], record.Bins, key)
	}

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(
		ctx,
		client,
		directory,
		dtBackupConfig(setName, class.size),
		dtRestoreConfig(client, class.size),
	)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(class.records), bStat.GetReadRecords())
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	// Validate records.
	after, err := readAllRecords(client, testASNamespace, setName)
	require.NoError(t, err)
	require.Equal(t, before.Len(), after.Len())

	for _, key := range keys {
		digest := string(key.Digest())

		expected, ok := before.Get(digest)
		require.Truef(t, ok, "record missing before backup: %v", key)

		actual, ok := after.Get(digest)
		require.Truef(t, ok, "record missing after restore: %v", key)

		dtRequireBinsEqual(t, expected.Bins, actual.Bins, key)

		// SendKey is set on write, so the user key travels through the backup
		// file and must come back unchanged.
		require.Equalf(t, expected.Key.Value(), actual.Key.Value(), "user key mismatch: %v", key)

		// Generation is not compared: restore rewrites the record, so a fresh
		// generation is expected. Void time is preserved and therefore is.
		require.Equalf(t, expected.Expiration, actual.Expiration, "expiration mismatch: %v", key)
	}
}

// dtSetName builds the Aerospike set name for one matrix cell.
func dtSetName(typeName, className string) string {
	return fmt.Sprintf("%s_%s_%s", dtSetPrefix, typeName, className)
}

// dtBackupConfig returns a backup config tuned for the given record size.
func dtBackupConfig(setName string, recordSize int) *ConfigBackup {
	cfg := NewDefaultBackupConfig()
	cfg.SetList = []string{setName}

	if recordSize >= dtLargeRecordThreshold {
		cfg.ParallelRead = 1
		cfg.ParallelWrite = 1
	}

	return cfg
}

// dtRestoreConfig returns a restore config tuned for the given record size.
// The batch size and the number of concurrent batches are scaled down for
// large records: the defaults keep hundreds of megabytes in flight and the
// server answers that with DEVICE_OVERLOAD.
//
// SendKey has to be enabled explicitly: when ConfigRestore.WritePolicy is nil
// the library falls back to the Aerospike client default write policy, and that
// default leaves SendKey off, so the user key carried by the backup file would
// never make it back into the database.
func dtRestoreConfig(client *a.Client, recordSize int) *ConfigRestore {
	// Copy, do not mutate: GetDefaultWritePolicy hands out the client's own policy.
	writePolicy := *client.GetDefaultWritePolicy()
	writePolicy.SendKey = true
	writePolicy.TotalTimeout = testTimeout
	writePolicy.SocketTimeout = testTimeout

	cfg := NewDefaultRestoreConfig()
	cfg.WritePolicy = &writePolicy
	cfg.RetryPolicy = models.NewDefaultRetryPolicy()

	if recordSize >= dtLargeRecordThreshold {
		cfg.BatchSize = max(1, dtInFlightBudget/recordSize)
		cfg.MaxAsyncBatches = dtLargeMaxAsyncBatches
	}

	return cfg
}

// dtWritePolicy is the write policy used by every writer in this test. Records
// are written one at a time with retries so that the large size buckets do not
// overload the server write queue.
func dtWritePolicy() *a.WritePolicy {
	policy := a.NewWritePolicy(0, 0)
	policy.SendKey = true
	policy.TotalTimeout = testTimeout
	policy.SocketTimeout = testTimeout
	policy.MaxRetries = dtMaxRetries
	policy.SleepBetweenRetries = dtSleepBetweenRetries

	return policy
}

// dtGenKeys builds deterministic keys cycling through every user key type
// Aerospike supports.
func dtGenKeys(set string, count int) ([]*a.Key, error) {
	keys := make([]*a.Key, count)

	for i := range count {
		var userKey any

		switch i % dtUserKeyKinds {
		case 0:
			userKey = i
		case 1:
			userKey = fmt.Sprintf("%s%d", dtUserKeyPrefix, i)
		default:
			userKey = fmt.Appendf(nil, "%s%d", dtUserKeyPrefix, i)
		}

		key, err := a.NewKey(testASNamespace, set, userKey)
		if err != nil {
			return nil, fmt.Errorf("failed to build key %d for set %s: %w", i, set, err)
		}

		keys[i] = key
	}

	return keys, nil
}

// dtGenWriter builds bin values on the client, which lets the test assert that
// the database gives back exactly what the test produced.
type dtGenWriter struct {
	gen func(size, idx int) a.BinMap
}

func (w dtGenWriter) Write(client *a.Client, keys []*a.Key, size int) (map[digestT]a.BinMap, error) {
	policy := dtWritePolicy()
	written := make(map[digestT]a.BinMap, len(keys))

	for i, key := range keys {
		bins := w.gen(size, i)
		if err := client.Put(policy, key, bins); err != nil {
			return nil, fmt.Errorf("failed to write record %s: %w", key, err)
		}

		written[string(key.Digest())] = bins
	}

	return written, nil
}

// dtHLLWriter creates HLL bins through the server. An HLL particle is built by
// the database and cannot be constructed on the client, so unlike every other
// type the expected value has to be read back right after the write.
type dtHLLWriter struct {
	indexBits   int
	minHashBits int
}

func (w dtHLLWriter) Write(client *a.Client, keys []*a.Key, _ int) (map[digestT]a.BinMap, error) {
	policy := dtWritePolicy()
	written := make(map[digestT]a.BinMap, len(keys))

	for i, key := range keys {
		// A distinct element per record so that no two HLL particles are equal.
		op := a.HLLAddOp(
			a.DefaultHLLPolicy(),
			dtBinHLL,
			[]a.Value{a.NewIntegerValue(i)},
			w.indexBits,
			w.minHashBits,
		)

		if _, err := client.Operate(policy, key, op); err != nil {
			return nil, fmt.Errorf("failed to create HLL bin for %s: %w", key, err)
		}

		record, err := client.Get(nil, key)
		if err != nil {
			return nil, fmt.Errorf("failed to read back HLL bin for %s: %w", key, err)
		}

		written[string(key.Digest())] = record.Bins
	}

	return written, nil
}

func dtGenInt(_, idx int) a.BinMap {
	return a.BinMap{dtBinInt: dtIntValues[idx%len(dtIntValues)]}
}

func dtGenFloat(_, idx int) a.BinMap {
	return a.BinMap{dtBinFloat: dtFloatValues[idx%len(dtFloatValues)]}
}

func dtGenBool(_, idx int) a.BinMap {
	return a.BinMap{dtBinBool: idx%2 == 0}
}

func dtGenString(size, idx int) a.BinMap {
	return a.BinMap{dtBinString: dtRandString(size, idx)}
}

func dtGenBlob(size, idx int) a.BinMap {
	return a.BinMap{dtBinBlob: dtRandBytes(size, idx)}
}

// dtGenList builds a nested, mixed-type list. Everything but the trailing
// filler is fixed, so the list exercises the same nesting at every size.
func dtGenList(size, idx int) a.BinMap {
	value := []any{
		dtIntValues[idx%len(dtIntValues)],
		dtFloatValues[idx%len(dtFloatValues)],
		dtRandString(dtNestedSize, idx),
		idx%2 == 0,
		nil,
		[]any{1, dtMapKeyNested, []byte(dtMapKeyNested), []any{1, 2, 3}},
		map[any]any{
			dtMapKeyIntValue: idx,
			dtMapKeyString:   dtRandString(dtNestedSize, idx+1),
			dtMapKeyNested:   []any{1, 2, 3},
		},
		dtRandBytes(dtPayloadSize(size), idx),
	}

	return a.BinMap{dtBinList: value}
}

// dtGenMap builds a nested map with both integer and string keys.
func dtGenMap(size, idx int) a.BinMap {
	value := map[any]any{
		dtMapKeyIntValue: idx,
		dtMapKeyInt:      dtIntValues[idx%len(dtIntValues)],
		dtMapKeyFloat:    dtFloatValues[idx%len(dtFloatValues)],
		dtMapKeyString:   dtRandString(dtNestedSize, idx),
		dtMapKeyBool:     idx%2 == 0,
		dtMapKeyNil:      nil,
		dtMapKeyList:     []any{1, dtMapKeyNested, []byte(dtMapKeyNested), map[any]any{1: dtMapKeyInt}},
		dtMapKeyNested:   map[any]any{1: 1, dtMapKeyString: []any{2, 2.5}},
		dtMapKeyPayload:  dtRandBytes(dtPayloadSize(size), idx),
	}

	return a.BinMap{dtBinMap: value}
}

// dtGenGeoJSON builds a valid GeoJSON polygon of roughly the requested size.
// The radius varies per record so that no two polygons are identical.
func dtGenGeoJSON(size, idx int) a.BinMap {
	vertices := max(dtGeoMinVertices, (size-len(dtGeoPrefix)-len(dtGeoSuffix))/dtGeoVertexLen)
	radius := dtGeoBaseRadius + float64(idx)/dtGeoRadiusStepPD

	var sb strings.Builder
	sb.Grow(size)
	sb.WriteString(dtGeoPrefix)

	// The ring has to be closed, so vertex 0 is emitted twice.
	for i := 0; i <= vertices; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}

		angle := 2 * math.Pi * float64(i%vertices) / float64(vertices)
		sb.WriteByte('[')
		sb.WriteString(strconv.FormatFloat(radius*math.Cos(angle), 'f', dtGeoPrecision, 64))
		sb.WriteByte(',')
		sb.WriteString(strconv.FormatFloat(radius*math.Sin(angle), 'f', dtGeoPrecision, 64))
		sb.WriteByte(']')
	}

	sb.WriteString(dtGeoSuffix)

	return a.BinMap{dtBinGeoJSON: a.GeoJSONValue(sb.String())}
}

// dtPayloadSize returns how many filler bytes a structured value may carry so
// that its total size lands close to size.
func dtPayloadSize(size int) int {
	return max(dtMinPayloadSize, size-dtStructureOverhead)
}

// dtRandString builds a deterministic string of exactly size bytes.
func dtRandString(size, idx int) string {
	rnd := rand.New(rand.NewPCG(dtSeed, uint64(idx)))

	var sb strings.Builder
	sb.Grow(size)

	for sb.Len() < size {
		r := dtStringAlphabet[rnd.IntN(len(dtStringAlphabet))]
		if sb.Len()+utf8.RuneLen(r) > size {
			// Pad the tail with single-byte runes to hit the target exactly.
			sb.WriteByte(dtPadByte)
			continue
		}

		sb.WriteRune(r)
	}

	return sb.String()
}

// dtRandBytes builds a deterministic blob of exactly size bytes. The first
// 256 bytes are 0x00..0xFF so that every byte value, NUL and LF included, is
// always present.
func dtRandBytes(size, idx int) []byte {
	rnd := rand.New(rand.NewPCG(dtSeed, uint64(idx)))
	buf := make([]byte, size)

	for i := range buf {
		if i < math.MaxUint8+1 {
			buf[i] = byte(i)
			continue
		}

		buf[i] = byte(rnd.IntN(math.MaxUint8 + 1))
	}

	return buf
}

// dtRequireBinsEqual compares two bin maps field by field, nested values
// included, without dumping megabytes of payload into the test log when they
// differ.
func dtRequireBinsEqual(t *testing.T, expected, actual a.BinMap, key *a.Key) {
	t.Helper()

	require.Equalf(t, dtBinNames(expected), dtBinNames(actual), "bin set mismatch: %v", key)

	for name, want := range expected {
		got := actual[name]
		if reflect.DeepEqual(want, got) {
			continue
		}

		t.Errorf("bin %q of %v does not match: expected %s, got %s",
			name, key, dtDescribe(want), dtDescribe(got))
	}
}

// dtBinNames returns the sorted bin names of a bin map.
func dtBinNames(bins a.BinMap) []string {
	names := make([]string, 0, len(bins))
	for name := range bins {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
}

// dtDescribe renders a bin value compactly: the large buckets would otherwise
// print megabytes on a failed assertion.
func dtDescribe(value any) string {
	switch v := value.(type) {
	case []byte:
		return fmt.Sprintf("[]byte(len=%d)", len(v))
	case string:
		return fmt.Sprintf("string(len=%d)", len(v))
	case a.HLLValue:
		return fmt.Sprintf("a.HLLValue(len=%d)", len(v))
	case a.GeoJSONValue:
		return fmt.Sprintf("a.GeoJSONValue(len=%d)", len(v))
	case []any:
		return fmt.Sprintf("[]any(len=%d)", len(v))
	case map[any]any:
		return fmt.Sprintf("map[any]any(len=%d)", len(v))
	default:
		return fmt.Sprintf("%T(%v)", value, value)
	}
}

// dtRaiseMaxRecordSize lifts the namespace record size limit to the 8 MiB
// ceiling Aerospike allows, so that the 7 MiB bucket can be written at all.
// max-record-size is a dynamic parameter, so no server restart is involved.
// The returned function puts the original value back.
func dtRaiseMaxRecordSize(t *testing.T, client *a.Client) func() {
	t.Helper()

	config, err := dtNamespaceConfig(client, testASNamespace)
	require.NoError(t, err)

	original, ok := config[dtMaxRecordSizeParam]
	require.Truef(t, ok, "namespace %s does not report %s", testASNamespace, dtMaxRecordSizeParam)

	require.NoError(t, dtSetNamespaceConfig(
		client, testASNamespace, dtMaxRecordSizeParam, strconv.Itoa(dtMaxRecordSize),
	))

	return func() {
		if err := dtSetNamespaceConfig(
			client, testASNamespace, dtMaxRecordSizeParam, original,
		); err != nil {
			t.Logf("failed to restore %s to %s: %v", dtMaxRecordSizeParam, original, err)
		}
	}
}

// dtSetNamespaceConfig applies a dynamic namespace parameter on every node and
// fails if any node rejects it.
func dtSetNamespaceConfig(client *a.Client, namespace, param, value string) error {
	command := fmt.Sprintf("set-config:context=namespace;id=%s;%s=%s", namespace, param, value)

	for _, node := range client.GetNodes() {
		response, err := node.RequestInfo(a.NewInfoPolicy(), command)
		if err != nil {
			return fmt.Errorf("node %s: failed to run %q: %w", node.GetName(), command, err)
		}

		if result := response[command]; !strings.EqualFold(result, dtInfoResponseOK) {
			return fmt.Errorf("node %s: %q returned %q", node.GetName(), command, result)
		}
	}

	return nil
}

// dtNamespaceConfig reads the namespace configuration from the first node.
func dtNamespaceConfig(client *a.Client, namespace string) (map[string]string, error) {
	nodes := client.GetNodes()
	if len(nodes) == 0 {
		return nil, errors.New("cluster reports no nodes")
	}

	command := fmt.Sprintf("get-config:context=namespace;id=%s", namespace)

	response, err := nodes[0].RequestInfo(a.NewInfoPolicy(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to run %q: %w", command, err)
	}

	pairs := strings.Split(response[command], ";")
	config := make(map[string]string, len(pairs))

	for _, pair := range pairs {
		name, value, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}

		config[name] = value
	}

	return config, nil
}

func TestBackupRestoreIndexUdf(t *testing.T) {
	t.Parallel()
	const setName = "testIndexUdf"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	restoreConfig := NewDefaultRestoreConfig()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	udfs := genUDFs()
	err = writeUDFs(asClient, udfs)
	require.NoError(t, err)

	indexes := genIndexes(testASNamespace, setName)
	err = writeSIndexes(asClient, indexes)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}

	// Validate stats.
	require.Equal(t, uint32(8), bStat.GetSIndexes())
	require.Equal(t, uint32(8), rStat.GetSIndexes())
	require.Equal(t, uint32(3), bStat.GetUDFs())
	require.Equal(t, uint32(3), rStat.GetUDFs())
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	// Validate sindexes.
	infoClient, err := testInfoClient(asClient)
	require.NoError(t, err)
	dbIndexes, err := readAllSIndexes(ctx, infoClient, testASNamespace)
	require.NoError(t, err)
	require.Equal(t, indexes, dbIndexes)
}

func TestBackupRestoreIOEncryptionFile(t *testing.T) {
	t.Parallel()
	const setName = "testEncryptionFile"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	privateKeyFile := "tests/pkey_test"

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.EncryptionPolicy = &EncryptionPolicy{
		KeyFile: &privateKeyFile,
		Mode:    EncryptAES128,
	}

	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.EncryptionPolicy = &EncryptionPolicy{
		KeyFile: &privateKeyFile,
		Mode:    EncryptAES128,
	}

	records, err := genRecords(testASNamespace, setName, 10_000, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreNamespace(t *testing.T) {
	t.Parallel()
	const setName = "testNamespace"

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.Namespace = testASNamespace
	backupConfig.SetList = []string{setName}

	ns := testASNamespace
	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.Namespace = &RestoreNamespaceConfig{
		Source:      &ns,
		Destination: &ns,
	}

	records, err := genRecords(testASNamespace, setName, 10_000, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreCompression(t *testing.T) {
	t.Parallel()
	const setName = "testCompression"

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.CompressionPolicy = NewCompressionPolicy(CompressZSTD, 20)

	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.CompressionPolicy = NewCompressionPolicy(CompressZSTD, 20)

	records, err := genRecords(testASNamespace, setName, 10_000, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreBinFilter(t *testing.T) {
	t.Parallel()
	const setName = "testBinFilter"

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.BinList = []string{"BackupRestore", "OnlyBackup"}

	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.BinList = []string{"BackupRestore", "OnlyRestore"}

	records, err := genRecords(testASNamespace, setName, 100, a.BinMap{
		"BackupRestore": 1,
		"OnlyBackup":    2,
		"OnlyRestore":   3,
	})
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	expected, err := genRecords(testASNamespace, setName, 100, a.BinMap{
		"BackupRestore": 1,
	})
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	require.Len(t, expected, dbRecords.Len())
	for _, expRec := range expected {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreTimestampFilter(t *testing.T) {
	t.Parallel()
	const setName = "testTimestampFilter"

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	// Generate diff batches of records.
	timeout := 1 * time.Second
	batch1, err := genRecords(testASNamespace, setName, 900, testBins)
	require.NoError(t, err)

	err = writeRecords(asClient, batch1)
	require.NoError(t, err)

	time.Sleep(timeout)
	lowerLimit := time.Now()
	batch2, err := genRecords(testASNamespace, setName, 600, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, batch2)
	require.NoError(t, err)

	time.Sleep(timeout)
	upperLimit := time.Now()
	time.Sleep(timeout)

	batch3, err := genRecords(testASNamespace, setName, 300, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, batch3)
	require.NoError(t, err)

	// every batch generated same records, but less of them each time.
	// batch1 contains too old values (many of them were overwritten).
	// batch3 contains too fresh values.
	var expected = tests.Subtract(batch2, batch3)

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.ModAfter = &lowerLimit
	backupConfig.ModBefore = &upperLimit

	restoreConfig := NewDefaultRestoreConfig()

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	require.Len(t, expected, dbRecords.Len())
	for _, expRec := range expected {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreRps(t *testing.T) {
	const (
		setName = "testRps"
		numRec  = 1000
		rps     = 200
	)
	// Extend timeout as we need ~11 seconds for test.
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout*2)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.ScanPolicy = a.NewScanPolicy()
	backupConfig.ScanPolicy.RecordsPerSecond = rps

	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.RecordsPerSecond = rps
	// Keep restore single-threaded so the client-side TPS limiter is the bottleneck.
	restoreConfig.Parallel = 1

	records, err := genRecords(testASNamespace, setName, numRec, a.BinMap{"a": "b"})
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	now := time.Now()
	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())
	totalDuration := time.Since(now)

	// rate.Limiter with burst=1 allows the first token immediately.
	minDuration := time.Duration(float64(numRec-1) / float64(rps) * float64(time.Second))
	const (
		minSlack = time.Second
		// Shared Aerospike and CI runners can add tail latency beyond the throttle floor.
		maxSlack = 5 * time.Second
	)

	assertDurationNearRps := func(t *testing.T, name string, got time.Duration) {
		t.Helper()
		require.GreaterOrEqual(t, got, minDuration-minSlack,
			"%s completed faster than the configured RPS allows", name)
		require.LessOrEqual(t, got, minDuration+maxSlack,
			"%s took longer than expected for the configured RPS", name)
	}

	assertDurationNearRps(t, "backup", bStat.GetDuration())
	assertDurationNearRps(t, "restore", rStat.GetDuration())
	require.InDelta(t, totalDuration, rStat.GetDuration()+bStat.GetDuration()+time.Second,
		float64(2*time.Second))

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())
}

func TestBackupRestoreNodeList(t *testing.T) {
	t.Parallel()
	const setName = "testNodeList"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	nodes := asClient.GetNodes()
	ic, err := asinfo.NewClient(asClient.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)
	nodeServiceAddress, err := ic.GetService(ctx, nodes[0].GetName())
	require.NoError(t, err)

	backupConfig := NewDefaultBackupConfig()
	backupConfig.NodeList = []string{nodeServiceAddress}
	backupConfig.SetList = []string{setName}
	restoreConfig := NewDefaultRestoreConfig()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())
}

func TestBackupRestoreRackList(t *testing.T) {
	t.Parallel()
	const setName = "testRackList"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.RackList = []int{0}
	backupConfig.SetList = []string{setName}
	restoreConfig := NewDefaultRestoreConfig()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())
}

func TestBackupRestorePartitionList(t *testing.T) {
	t.Parallel()
	const setName = "testPartList"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	digest := base64.StdEncoding.EncodeToString(records[0].Key.Digest())
	digestFilter, err := NewPartitionFilterByDigest(testASNamespace, digest)
	require.NoError(t, err)

	records = []*a.Record{records[0]}

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.ParallelRead = 4
	backupConfig.PartitionFilters = []*a.PartitionFilter{
		NewPartitionFilterByID(1),
		NewPartitionFilterByRange(2, 3),
		digestFilter,
	}

	restoreConfig := NewDefaultRestoreConfig()

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())
}

func TestBackupRestoreAfterDigest(t *testing.T) {
	t.Parallel()
	const setName = "testAfterDigest"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	digest := base64.StdEncoding.EncodeToString(records[0].Key.Digest())
	afterDigestFilter, err := NewPartitionFilterAfterDigest(testASNamespace, digest)
	require.NoError(t, err)

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.PartitionFilters = []*a.PartitionFilter{
		afterDigestFilter,
	}

	restoreConfig := NewDefaultRestoreConfig()

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	require.Less(t, dbRecords.Len(), len(records))

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())
}

func TestBackupRestoreDefault(t *testing.T) {
	t.Parallel()
	const setName = "testDefault"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	restoreConfig := NewDefaultRestoreConfig()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	dirSize := uint64(tests.DirSize(directory))
	require.Equal(t, dirSize, bStat.GetBytesWritten())
	require.Less(t, rStat.GetTotalBytesRead(), dirSize) // restore size doesn't include asb control characters

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreDisableBatchWrites(t *testing.T) {
	t.Parallel()
	const setName = "testDisableBatchWrites"
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.DisableBatchWrites = true

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreFileLimit(t *testing.T) {
	t.Parallel()
	const setName = "testFileLimit"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.FileLimit = 1024 * 1024
	restoreConfig := NewDefaultRestoreConfig()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	backupFiles, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Equal(t, uint64(len(backupFiles)), bStat.GetFileCount())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreFileLimitDisableBatch(t *testing.T) {
	t.Parallel()
	const setName = "testFileLimitDisableBatch"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.FileLimit = 1024 * 1024

	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.DisableBatchWrites = true

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	backupFiles, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Equal(t, uint64(len(backupFiles)), bStat.GetFileCount())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreParallelDisableBatch(t *testing.T) {
	t.Parallel()
	const setName = "testParallelDBW"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.ParallelWrite = 100

	restoreConfig := NewDefaultRestoreConfig()
	restoreConfig.DisableBatchWrites = true

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	backupFiles, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Equal(t, uint64(len(backupFiles)), bStat.GetFileCount())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreParallelFileLimit(t *testing.T) {
	t.Parallel()
	const setName = "testParallelFileLimit"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.FileLimit = 1024 * 1024
	backupConfig.ParallelWrite = 100

	restoreConfig := NewDefaultRestoreConfig()

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	backupFiles, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Equal(t, uint64(len(backupFiles)), bStat.GetFileCount())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreWithPartitions(t *testing.T) {
	t.Parallel()
	const setName = "testWithPartitions"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	// backup half the partitions
	startPartition := 256
	partitionCount := 2056

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.PartitionFilters = []*a.PartitionFilter{NewPartitionFilterByRange(startPartition, partitionCount)}

	restoreConfig := NewDefaultRestoreConfig()

	numRec := 100
	bins := a.BinMap{
		"IntBin": 1,
	}
	records, err := genRecords(testASNamespace, setName, numRec, bins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	recsByPartition := make(map[int][]*a.Record)

	for _, rec := range records {
		partitionID := rec.Key.PartitionId()

		if _, ok := recsByPartition[partitionID]; !ok {
			recsByPartition[partitionID] = []*a.Record{}
		}

		recsByPartition[partitionID] = append(recsByPartition[partitionID], rec)
	}

	// reset the expected record count
	numRec = 0

	records = []*a.Record{}
	for pid, recs := range recsByPartition {
		if pid >= startPartition && pid < startPartition+partitionCount {
			numRec += len(recs)
			records = append(records, recs...)
		}
	}

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	bStat, rStat, err := runBackupRestoreLocal(ctx, asClient, directory, backupConfig, restoreConfig)
	require.NoError(t, err)
	require.Equal(t, bStat.GetReadRecords(), rStat.GetRecordsInserted())

	// Validate records.
	dbRecords, err := readAllRecords(asClient, testASNamespace, setName)
	require.NoError(t, err)

	// Validate stats.
	require.Equal(t, uint64(0), rStat.GetRecordsExpired())
	require.Equal(t, uint64(0), rStat.GetRecordsSkipped())
	require.Equal(t, uint64(0), rStat.GetRecordsFresher())
	require.Equal(t, uint64(0), rStat.GetRecordsExisted())
	require.Equal(t, uint64(0), rStat.GetRecordsIgnored())

	backupFiles, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Equal(t, uint64(len(backupFiles)), bStat.GetFileCount())

	require.Len(t, records, dbRecords.Len())
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.Equal(t, expRec.Bins, actual.Bins)
	}
}

func TestRestoreExpiredRecords(t *testing.T) {
	t.Parallel()
	const setName = "TestRestoreExpiredRecords"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	numRec := 100
	bins := a.BinMap{
		"IntBin": 1,
	}
	recs, err := genRecords(testASNamespace, setName, numRec, bins)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	writer, err := local.NewWriter(
		ctx,
		options.WithDir(directory),
	)
	require.NoError(t, err)
	w, err := writer.NewWriter(ctx, fmt.Sprintf("%s-%s.asb", testASNamespace, setName))
	require.NoError(t, err)
	require.NotNil(t, w)

	encoder := NewEncoder(testASNamespace, false, models.SIndexInfo{})

	header := encoder.GetHeader(true)

	_, err = w.Write(header)
	require.NoError(t, err)

	for _, rec := range recs {
		modelRec := &models.Record{
			Record: rec,
			// guaranteed to be expired
			VoidTime: 1,
		}

		token := models.NewRecordToken(modelRec, 0, nil)
		encoded, err := encoder.EncodeToken(token, nil)
		require.NoError(t, err)

		_, err = w.Write(encoded)
		require.NoError(t, err)
	}

	err = w.Close()
	require.NoError(t, err)

	reader, err := local.NewReader(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithDir(directory),
	)
	require.NoError(t, err)

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)
	defer testAeroClient.Close()

	backupClient, err := NewClient(testAeroClient)
	require.NoError(t, err)

	rh, err := backupClient.Restore(
		ctx,
		NewDefaultRestoreConfig(),
		reader,
	)
	require.NoError(t, err)
	require.NotNil(t, rh)

	err = rh.Wait(ctx)
	require.NoError(t, err)

	statsRestore := rh.GetStats()
	require.NotNil(t, statsRestore)
	require.Equal(t, uint64(numRec), statsRestore.GetReadRecords())
	require.Equal(t, uint64(numRec), statsRestore.GetRecordsExpired())
}

func TestBackupContextCancel(t *testing.T) {
	t.Parallel()
	const setName = "TestBackupContextCancel"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupClient, err := NewClient(asClient)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))
	err = os.MkdirAll(directory, os.ModePerm)
	require.NoError(t, err)

	reader, err := local.NewReader(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithDir(directory),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	rh, err := backupClient.Restore(
		ctx,
		NewDefaultRestoreConfig(),
		reader,
	)
	require.NoError(t, err)
	require.NotNil(t, rh)

	err = rh.Wait(ctx)
	require.Error(t, err)
}

func TestRestoreContextCancel(t *testing.T) {
	t.Parallel()
	const setName = "TestRestoreContextCancel"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupClient, err := NewClient(asClient)
	require.NoError(t, err)

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))
	err = os.MkdirAll(directory, os.ModePerm)
	require.NoError(t, err)

	writer, err := local.NewWriter(
		ctx,
		options.WithDir(directory),
	)
	require.NoError(t, err)

	bh, err := backupClient.Backup(
		ctx,
		NewDefaultBackupConfig(),
		writer,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, bh)

	err = bh.Wait(ctx)
	require.Error(t, err)
}

func TestBackupEstimate(t *testing.T) {
	t.Parallel()
	const setName = "testEstimate"
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(func() { cancel() })

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	t.Cleanup(func() { asClient.Close() })

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	backupClient, err := NewClient(asClient)
	require.NoError(t, err)

	configFileLimit := NewDefaultBackupConfig()
	configFileLimit.FileLimit = 5

	testCases := []struct {
		name   string
		config *ConfigBackup
	}{
		{
			"configDefault",
			NewDefaultBackupConfig(),
		},
		{
			"configFileLimit",
			configFileLimit,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bh, err := backupClient.Estimate(
				ctx,
				tt.config,
				10,
			)
			require.NoError(t, err)
			require.NotNil(t, bh)
		})
	}
}

func TestBackupContinuation(t *testing.T) {
	t.Parallel()

	const (
		setName       = "testBackupContinuation"
		totalRecords  = 900
		testStateFile = "test_state_file"
	)

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	batch, err := genRecords(testASNamespace, setName, totalRecords, testBins)
	require.NoError(t, err)

	err = writeRecords(asClient, batch)
	require.NoError(t, err)

	testFolder := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))
	err = os.MkdirAll(testFolder, os.ModePerm)
	require.NoError(t, err)
	stateFile := path.Join(testFolder, testStateFile)

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	t.Log("first backup")
	first, err := runFirstBackup(ctx, asClient, setName, testFolder, stateFile)
	require.NoError(t, err)
	t.Log("first backup finished")

	ctx = t.Context()
	t.Log("continue backup")
	second, err := runContinueBackup(ctx, asClient, setName, testFolder, stateFile)
	require.NoError(t, err)
	t.Log("continue backup finished")

	t.Log("first:", first, "second:", second)
	t.Log(first + second)
	require.GreaterOrEqual(t, first+second, uint64(totalRecords))
}

func runFirstBackup(ctx context.Context, asClient *a.Client, setName, testFolder, testStateFile string,
) (uint64, error) {
	writers, err := local.NewWriter(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithSkipDirCheck(),
		options.WithDir(testFolder),
	)
	if err != nil {
		return 0, err
	}

	readers, err := local.NewReader(
		ctx,
		options.WithDir(testFolder),
		options.WithSkipDirCheck(),
	)
	if err != nil {
		return 0, err
	}

	backupCfg := NewDefaultBackupConfig()
	backupCfg.Namespace = testASNamespace
	backupCfg.SetList = []string{setName}
	backupCfg.ParallelRead = 1
	backupCfg.ParallelWrite = 1
	backupCfg.Bandwidth = 100000

	backupCfg.StateFile = testStateFile
	backupCfg.FileLimit = 10
	backupCfg.PageSize = 1

	backupClient, err := NewClient(asClient)
	if err != nil {
		return 0, err
	}

	backupHandler, err := backupClient.Backup(ctx, backupCfg, writers, readers)
	if err != nil {
		return 0, err
	}

	// use backupHandler.Wait() to wait for the job to finish or fail
	err = backupHandler.Wait(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return 0, err
	}

	return backupHandler.GetStats().GetReadRecords(), nil
}

func runContinueBackup(ctx context.Context, asClient *a.Client, setName, testFolder, testStateFile string,
) (uint64, error) {
	writers, err := local.NewWriter(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithSkipDirCheck(),
		options.WithDir(testFolder),
	)
	if err != nil {
		return 0, err
	}

	readers, err := local.NewReader(
		ctx,
		options.WithDir(testFolder),
	)
	if err != nil {
		return 0, err
	}

	backupCfg := NewDefaultBackupConfig()
	backupCfg.Namespace = testASNamespace
	backupCfg.SetList = []string{setName}
	backupCfg.ParallelRead = 1
	backupCfg.ParallelWrite = 1

	backupCfg.StateFile = testStateFile
	backupCfg.Continue = true
	backupCfg.FileLimit = 100000
	backupCfg.PageSize = 100

	backupClient, err := NewClient(asClient)
	if err != nil {
		return 0, err
	}

	backupHandler, err := backupClient.Backup(ctx, backupCfg, writers, readers)
	if err != nil {
		return 0, err
	}

	// use backupHandler.Wait() to wait for the job to finish or fail
	err = backupHandler.Wait(ctx)
	if err != nil {
		return 0, err
	}

	return backupHandler.GetStats().GetReadRecords(), nil
}

//nolint:unparam // In future we will use different namespaces for tests.
func genRecords(namespace, set string, numRec int, bins a.BinMap) ([]*a.Record, error) {
	userKeys := []any{1, "string", []byte("bytes")}
	recs := make([]*a.Record, numRec)
	for i := range numRec {
		userKey := userKeys[i%len(userKeys)]
		switch k := userKey.(type) {
		case int:
			userKey = i
		case string:
			userKey = k + fmt.Sprint(i)
		case []byte:
			k = fmt.Appendf(k, "%d", i)
			userKey = k
		}
		key, err := a.NewKey(namespace, set, userKey)
		if err != nil {
			return nil, err
		}

		recs[i] = &a.Record{
			Key:  key,
			Bins: bins,
		}
	}
	return recs, nil
}

func genIndexes(namespace, set string) []*models.SIndex {
	listCtx, _ := a.CDTContextToBase64([]*a.CDTContext{a.CtxListValue(a.NewValue([]byte("hi")))})
	mapKeyCTX, _ := a.CDTContextToBase64([]*a.CDTContext{a.CtxMapKey(a.NewValue(1))})
	mapValueCTX, _ := a.CDTContextToBase64([]*a.CDTContext{a.CtxMapValue(a.NewValue("hi"))})

	return []*models.SIndex{
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "IntBinIndex"),
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "IntBin",
				BinType: models.NumericSIDataType,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "StringBinIndex"),
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "StringBin",
				BinType: models.StringSIDataType,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "ListBinIndex"),
			IndexType: models.ListElementSIndex,
			Path: models.SIndexPath{
				BinName: "ListBin",
				BinType: models.NumericSIDataType,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "MapBinIndex"),
			IndexType: models.MapKeySIndex,
			Path: models.SIndexPath{
				BinName: "MapBin",
				BinType: models.StringSIDataType,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "GeoJSONBinIndex"),
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "GeoJSONBin",
				BinType: models.GEO2DSphereSIDataType,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "ListElemBinIndex"),
			IndexType: models.ListElementSIndex,
			Path: models.SIndexPath{
				BinName:    "ListBin",
				BinType:    models.BlobSIDataType,
				B64Context: listCtx,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "MapKeyBinIndex"),
			IndexType: models.MapKeySIndex,
			Path: models.SIndexPath{
				BinName:    "MapBin",
				BinType:    models.NumericSIDataType,
				B64Context: mapKeyCTX,
			},
		},
		{
			Namespace: namespace,
			Set:       set,
			Name:      fmt.Sprintf("%s%s", set, "MapValBinIndex"),
			IndexType: models.MapValueSIndex,
			Path: models.SIndexPath{
				BinName:    "MapBin",
				BinType:    models.StringSIDataType,
				B64Context: mapValueCTX,
			},
		},
	}
}

func genUDFs() []*models.UDF {
	return []*models.UDF{
		{
			Name:    "simple_func.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte("function test(rec)\n  return 1\nend"),
		},
		{
			Name:    "test.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte(tests.UDF),
		},
		{
			Name:    "add.lua",
			UDFType: models.UDFTypeLUA,
			Content: []byte("function add(rec)\n  return 1 + 1\nend\n"),
		},
	}
}

func writeRecords(client *a.Client, recs []*a.Record) error {
	bp := client.GetDefaultBatchPolicy()
	bp.TotalTimeout = testTimeout
	bp.SocketTimeout = testTimeout

	bwp := client.GetDefaultBatchWritePolicy()
	bwp.SendKey = true

	writeOps := make([]a.BatchRecordIfc, 0, len(recs))
	for _, rec := range recs {
		ops := make([]*a.Operation, 0, len(rec.Bins))
		for k, v := range rec.Bins {
			ops = append(ops, a.PutOp(a.NewBin(k, v)))
		}
		writeOps = append(writeOps, a.NewBatchWrite(bwp, rec.Key, ops...))
	}

	return client.BatchOperate(bp, writeOps)
}

func writeUDFs(client *a.Client, udfs []*models.UDF) error {
	for _, udf := range udfs {
		var UDFLang a.Language

		switch udf.UDFType {
		case models.UDFTypeLUA:
			UDFLang = a.LUA
		default:
			return errors.New("failed to register UDF: invalid UDF language")
		}

		job, err := client.RegisterUDF(nil, udf.Content, udf.Name, UDFLang)
		if err != nil {
			return err
		}

		errs := job.OnComplete()
		if err := <-errs; err != nil {
			return err
		}
	}

	return nil
}

func writeSIndexes(client *a.Client, sindexes []*models.SIndex) error {
	for _, sindex := range sindexes {
		sindexType, err := getIndexType(sindex)
		if err != nil {
			return err
		}

		sindexCollectionType, err := getSindexCollectionType(sindex)
		if err != nil {
			return err
		}

		var ctx []*a.CDTContext
		if sindex.Path.B64Context != "" {
			var err error
			ctx, err = a.Base64ToCDTContext(sindex.Path.B64Context)
			if err != nil {
				return err
			}
		}

		task, err := client.CreateComplexIndex(
			nil,
			sindex.Namespace,
			sindex.Set,
			sindex.Name,
			sindex.Path.BinName,
			sindexType,
			sindexCollectionType,
			ctx...,
		)
		if err != nil {
			return err
		}

		errs := task.OnComplete()
		if err := <-errs; err != nil {
			return err
		}
	}

	return nil
}

func getSindexCollectionType(sindex *models.SIndex) (a.IndexCollectionType, error) {
	switch sindex.IndexType {
	case models.BinSIndex:
		return a.ICT_DEFAULT, nil
	case models.ListElementSIndex:
		return a.ICT_LIST, nil
	case models.MapKeySIndex:
		return a.ICT_MAPKEYS, nil
	case models.MapValueSIndex:
		return a.ICT_MAPVALUES, nil
	}
	return 0, fmt.Errorf("invalid sindex collection type: %c", sindex.IndexType)
}

func getIndexType(sindex *models.SIndex) (a.IndexType, error) {
	switch sindex.Path.BinType {
	case models.NumericSIDataType:
		return a.NUMERIC, nil
	case models.StringSIDataType:
		return a.STRING, nil
	case models.BlobSIDataType:
		return a.BLOB, nil
	case models.GEO2DSphereSIDataType:
		return a.GEO2DSPHERE, nil
	}
	return "", fmt.Errorf("invalid sindex bin type: %c", sindex.Path.BinType)
}

func readAllSIndexes(ctx context.Context, client *asinfo.Client, namespace string) ([]*models.SIndex, error) {
	return client.GetSIndexes(ctx, namespace)
}

type digestT = string

// RecordMap is a thread-safe map of record digests to records.
type RecordMap struct {
	mu   sync.RWMutex
	data map[digestT]*a.Record
}

// NewRecordMap creates a new thread-safe RecordMap
func NewRecordMap() *RecordMap {
	return &RecordMap{
		data: make(map[digestT]*a.Record),
	}
}

// Get retrieves a record by digest
func (rm *RecordMap) Get(d digestT) (*a.Record, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	rec, ok := rm.data[d]
	return rec, ok
}

// Set stores a record with its digest
func (rm *RecordMap) Set(d digestT, rec *a.Record) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.data[d] = rec
}

// Len returns the number of records
func (rm *RecordMap) Len() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.data)
}

//nolint:unparam // In future tests will use different namespaces.
func readAllRecords(client *a.Client, namespace, set string) (*RecordMap, error) {
	records := NewRecordMap()
	stmt := a.NewStatement(namespace, set)

	rset, err := client.Query(nil, stmt)
	if err != nil {
		return nil, err
	}
	rchan := rset.Results()
	for r := range rchan {
		if r.Err != nil {
			return nil, r.Err
		}
		if r.Record.Key.SetName() == set {
			records.Set(string(r.Record.Key.Digest()), r.Record)
		}
	}

	return records, nil
}
