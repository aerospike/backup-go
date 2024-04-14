package main

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"math/rand"
	"os"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
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
	encoder, _ := encoding.NewASBEncoderFactory().CreateEncoder()
	test := []struct {
		name          string
		readers       backup.ReaderFactory
		writerFactory backup.WriteFactory
	}{
		{
			name:          "directory",
			readers:       backup.NewFileReaderFactory("test_data", encoding.NewASBDecoderFactory()),
			writerFactory: HandleError(backup.NewDirectoryWriterFactory("test_data", 1024*1024+1, encoder)),
		},
		{
			name:          "s3",
			readers:       backup.NewS3ReaderFactory(s3Cfg, encoding.NewASBDecoderFactory()),
			writerFactory: backup.NewS3WriterFactory(s3Cfg, encoder),
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			values := prepareData()
			runBackup(t, backupClient, tc.writerFactory)
			_ = aerospikeClient.Truncate(nil, backupCfg.Namespace, "", nil)
			runRestore(t, backupClient, tc.readers)
			validateData(t, values)
		})
	}

}

func prepareData() map[*aerospike.Key]*aerospike.Bin {
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
	ctx := context.Background()
	handler, err := backupClient.Backup(ctx, backupCfg, factory)
	assert.NoError(t, err)
	_ = handler.Wait(ctx)
}

func runRestore(t *testing.T, backupClient *backup.Client, readers backup.ReaderFactory) {
	ctx := context.Background()
	restoreHandler, err := backupClient.RestoreGeneric(ctx, restoreConfig, readers)
	assert.NoError(t, err)
	err = restoreHandler.Wait(ctx)
	assert.NoError(t, err)
	assert.Equal(t, RecordNumber, restoreHandler.GetStats().GetRecords())
}

func validateData(t *testing.T, values map[*aerospike.Key]*aerospike.Bin) {
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
