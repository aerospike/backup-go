package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
	"github.com/stretchr/testify/assert"
)

const RecordNumber uint64 = 100
const binName = "randInt"

var s3Cfg = &backup.S3Config{
	Bucket:   "as-backup-bucket",
	Region:   "eu",
	Endpoint: "http://localhost:9000",
	Profile:  "minio",
	Prefix:   "test",
}

var backupCfg = backupConfig()
var aerospikeClient = makeAerospikeClient()
var restoreConfig = backup.NewRestoreConfig()

func TestBackupRestore(t *testing.T) {
	backupClient, err := backup.NewClient(aerospikeClient, "id", slog.Default(), nil)
	if err != nil {
		panic(err)
	}

	_ = os.RemoveAll("test_data")
	test := []struct {
		name          string
		readers       backup.ReaderFactory
		writerFactory backup.WriteFactory
	}{
		{
			name:    "directory",
			readers: backup.NewFileReaderFactory("test_data", encoding.NewASBDecoderFactory()),
			writerFactory: HandleError(backup.NewDirectoryWriterFactory(
				"test_data", 1024*1024+1, encoding.NewASBEncoderFactory())),
		},
		{
			name:          "s3",
			readers:       backup.NewS3ReaderFactory(s3Cfg, encoding.NewASBDecoderFactory()),
			writerFactory: backup.NewS3WriterFactory(s3Cfg, encoding.NewASBEncoderFactory()),
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			values := prepareData(t)
			runBackup(t, backupClient, tc.writerFactory)
			_ = aerospikeClient.Truncate(nil, backupCfg.Namespace, "", nil)
			runRestore(t, backupClient, tc.readers)
			validateData(t, values)
		})
	}
}

func prepareData(t *testing.T) map[*aerospike.Key]*aerospike.Bin {
	t.Helper()
	var values = make(map[*aerospike.Key]*aerospike.Bin)
	_ = aerospikeClient.Truncate(nil, backupCfg.Namespace, "", nil)
	for i := range RecordNumber {
		key, _ := aerospike.NewKey(backupCfg.Namespace, "test", fmt.Sprintf("key%d", i))
		bin := aerospike.NewBin(binName, rand.Int())
		values[key] = bin
		_ = aerospikeClient.AddBins(nil, key, bin)
	}
	return values
}

func runBackup(t *testing.T, backupClient *backup.Client, factory backup.WriteFactory) {
	t.Helper()
	ctx := context.Background()
	handler, err := backupClient.Backup(ctx, backupCfg, factory)
	assert.NoError(t, err)
	_ = handler.Wait(ctx)
}

func runRestore(t *testing.T, backupClient *backup.Client, readers backup.ReaderFactory) {
	t.Helper()
	ctx := context.Background()
	restoreHandler, err := backupClient.Restore(ctx, restoreConfig, readers)
	assert.NoError(t, err)
	err = restoreHandler.Wait(ctx)
	assert.NoError(t, err)
	assert.Equal(t, RecordNumber, restoreHandler.GetStats().GetRecords())
}

func validateData(t *testing.T, values map[*aerospike.Key]*aerospike.Bin) {
	t.Helper()
	for key, val := range values {
		record, err := aerospikeClient.Get(nil, key, binName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(record.Bins))
		assert.Equal(t, val.Value.GetObject(), record.Bins[binName])
	}
}

func backupConfig() *backup.BackupConfig {
	backupCfg := backup.NewBackupConfig()
	backupCfg.Namespace = "source-ns1"
	backupCfg.Parallel = 2
	backupCfg.EncoderFactory = encoding.NewASBEncoderFactory()
	return backupCfg
}

func makeAerospikeClient() *aerospike.Client {
	policy := aerospike.NewClientPolicy()
	policy.User = "tester"
	policy.Password = "psw"
	aerospikeClient, aerr := aerospike.NewClientWithPolicy(policy, "127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}
	return aerospikeClient
}

func HandleError[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}

	return value
}
