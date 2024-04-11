package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
)

func main() {
	policy := aerospike.NewClientPolicy()
	policy.User = "tester"
	policy.Password = "psw"
	aerospikeClient, aerr := aerospike.NewClientWithPolicy(policy, "127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	// BACKUP
	backupClientConfig := backup.NewConfig()

	backupClient, err := backup.NewClient(aerospikeClient, "id", slog.Default(), backupClientConfig)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewBackupConfig()
	backupCfg.Namespace = "source-ns1"
	backupCfg.Parallel = 2
	backupCfg.EncoderFactory = encoding.NewASBEncoderFactory()
	ctx := context.Background()

	// BACKUP LOCAL
	os.RemoveAll("test")
	encoder, _ := encoding.NewASBEncoderFactory().CreateEncoder()
	writerFactory, err := backup.NewDirectoryWriterFactory("test", 0, encoder)
	if err != nil {
		panic(err)
	}

	handler, err := backupClient.BackupGeneric(ctx, backupCfg, writerFactory)
	if err != nil {
		panic(err)
	}

	handler.Wait(ctx)
	slog.Info("backup file", "stats", handler.GetStats())

	// BACKUP S3
	s3Cfg := &backup.S3Config{
		Bucket:   "as-backup-bucket",
		Region:   "eu",
		Endpoint: "http://localhost:9000",
		Profile:  "minio",
		Prefix:   "test",
	}
	writerFactoryS3, err := backup.NewS3WriterFactory(s3Cfg, encoder)
	if err != nil {
		panic(err)
	}
	s3handler, err := backupClient.BackupGeneric(ctx, backupCfg, writerFactoryS3)
	s3handler.Wait(ctx)

	slog.Info("backup s3", "stats", s3handler.GetStats())

	// RESTORE
	restoreConfig := backup.NewRestoreConfig()

	// RESTORE LOCAL
	readerFactory, err := backup.NewFileReaderFactory("test", encoding.NewASBDecoderFactory())
	if err != nil {
		panic(err)
	}
	ctx = context.Background()
	restoreHandler, err := backupClient.RestoreGeneric(ctx, restoreConfig, readerFactory)
	if err != nil {
		panic(err)
	}
	restoreHandler.Wait(ctx)

	slog.Info("restore file", "stats", restoreHandler.GetStats())
	//
	// RESTORE S3
	s3ReaderFactory := backup.NewS3ReaderFactory(s3Cfg, encoding.NewASBDecoderFactory())
	ctx = context.Background()
	restoreS3Handler, err := backupClient.RestoreGeneric(ctx, restoreConfig, s3ReaderFactory)
	if err != nil {
		panic(err)
	}
	restoreS3Handler.Wait(ctx)
	slog.Info("restore s3", "stats", restoreS3Handler.GetStats())
}
