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

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

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
	return asinfo.NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy())
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

	backupClient, err := NewClient(client, WithID("test_client"))
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

func TestBackupRestoreIndexUdf(t *testing.T) {
	t.Parallel()
	const setName = "testIndexUdf"
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	require.EqualValues(t, indexes, dbIndexes)
}

func TestBackupRestoreIOEncryptionFile(t *testing.T) {
	t.Parallel()
	const setName = "testEncryptionFile"
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
	for _, expRec := range records {
		actual, ok := dbRecords.Get(string(expRec.Key.Digest()))
		if !ok {
			t.Errorf("expected record not found: %v", expRec.Key)
			return
		}
		require.EqualValues(t, expRec.Bins, actual.Bins)
	}
}

func TestBackupRestoreBinFilter(t *testing.T) {
	t.Parallel()
	const setName = "testBinFilter"

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(expected))
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

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(expected))
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
	t.Parallel()
	const (
		setName = "testRps"
		numRec  = 1000
		rps     = 200
		epsilon = 2 * float64(time.Second)
	)
	// Extend timeout as we need ~11 seconds for test.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout*2)
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

	expectedDuration := time.Duration(1000.0*numRec/rps) * time.Millisecond

	// Validate records.
	require.InDelta(t, expectedDuration, bStat.GetDuration(), epsilon)
	require.InDelta(t, expectedDuration, rStat.GetDuration(), epsilon)
	require.InDelta(t, totalDuration, rStat.GetDuration()+bStat.GetDuration(), epsilon)

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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	nodes := asClient.GetNodes()
	ic, err := asinfo.NewClient(asClient.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy())
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	require.Equal(t, dbRecords.Len(), len(records))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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
	w, err := writer.NewWriter(ctx, fmt.Sprintf("%s-%s.asb", testASNamespace, setName), false)
	require.NoError(t, err)
	require.NotNil(t, w)

	encoder := NewEncoder[*models.Token](EncoderTypeASB, testASNamespace, false, false)

	header := encoder.GetHeader(0, false)

	_, err = w.Write(header)
	require.NoError(t, err)

	for _, rec := range recs {
		modelRec := &models.Record{
			Record: rec,
			// guaranteed to be expired
			VoidTime: 1,
		}

		token := models.NewRecordToken(modelRec, 0, nil)
		v, err := encoder.EncodeToken(token)
		require.NoError(t, err)

		_, err = w.Write(v)
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

	backupClient, err := NewClient(testAeroClient, WithID("test_client"))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupClient, err := NewClient(asClient, WithID("test_client"))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	cancel()

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	defer asClient.Close()

	backupClient, err := NewClient(asClient, WithID("test_client"))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(func() { cancel() })

	asClient, err := testAerospikeClient()
	require.NoError(t, err)
	t.Cleanup(func() { asClient.Close() })

	records, err := genRecords(testASNamespace, setName, 100, testBins)
	require.NoError(t, err)
	err = writeRecords(asClient, records)
	require.NoError(t, err)

	backupClient, err := NewClient(asClient, WithID("test_client"))
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

	for i := 0; i < 5; i++ {
		testFolder := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))
		err = os.MkdirAll(testFolder, os.ModePerm)
		require.NoError(t, err)
		stateFile := path.Join(testFolder, testStateFile)

		ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
		t.Log("first backup")
		first, err := runFirstBackup(ctx, asClient, setName, testFolder, stateFile)
		require.NoError(t, err)
		t.Log("first backup finished")

		ctx = context.Background()
		t.Log("continue backup")
		second, err := runContinueBackup(ctx, asClient, setName, testFolder, stateFile)
		require.NoError(t, err)
		t.Log("continue backup finished")

		t.Log("first:", first, "second:", second)
		t.Log(first + second)
		require.GreaterOrEqual(t, first+second, uint64(totalRecords))
	}
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

	backupClient, err := NewClient(asClient, WithID("test_client"))
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

	backupClient, err := NewClient(asClient, WithID("test_client"))
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
	for i := 0; i < numRec; i++ {
		userKey := userKeys[i%len(userKeys)]
		switch k := userKey.(type) {
		case int:
			userKey = i
		case string:
			userKey = k + fmt.Sprint(i)
		case []byte:
			k = append(k, []byte(fmt.Sprint(i))...)
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
			return errors.New("error registering UDF: invalid UDF language")
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
