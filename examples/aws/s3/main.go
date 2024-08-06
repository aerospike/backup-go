package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/aws/s3"
)

const (
	dbHost       = "127.0.0.1"
	dbPort       = 3000
	dbUser       = "tester"
	dbPass       = "psw"
	dbNameSource = "source-ns1"
	dbNameTarget = "source-ns2"
	backupFolder = "backup_folder"
)

func main() {
	ctx := context.Background()
	backupClient := initBackupClient()

	runBackup(ctx, backupClient)

	runRestore(ctx, backupClient)
}

func initBackupClient() *backup.Client {
	policy := aerospike.NewClientPolicy()
	policy.User = dbUser
	policy.Password = dbPass

	aerospikeClient, aerr := aerospike.NewClientWithPolicy(
		policy,
		dbHost,
		dbPort,
	)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backup.NewClient(aerospikeClient, "client_id", slog.Default())
	if err != nil {
		panic(err)
	}

	return backupClient
}

func runBackup(ctx context.Context, c *backup.Client) {
	s3cfg := &s3.Config{
		Bucket:   "backup",
		Region:   "eu-central-1",
		Endpoint: "http://localhost:9000",
		Profile:  "minio",
		Prefix:   "test",
	}

	writers, err := backup.NewWriterS3(ctx, s3cfg, true)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = dbNameSource
	backupCfg.Parallel = 10

	// set compression policy
	backupCfg.CompressionPolicy = &backup.CompressionPolicy{
		Mode: backup.CompressZSTD,
	}

	backupHandler, err := c.Backup(ctx, backupCfg, writers)
	if err != nil {
		panic(err)
	}

	// use backupHandler.Wait() to wait for the job to finish or fail
	err = backupHandler.Wait(ctx)
	if err != nil {
		log.Printf("Backup failed: %v", err)
	}

	fmt.Println("backup done")
}

func runRestore(ctx context.Context, c *backup.Client) {
	s3cfg := &s3.Config{
		Bucket:   "backup",
		Region:   "eu-central-1",
		Endpoint: "http://localhost:9000",
		Profile:  "minio",
		Prefix:   "test",
	}

	readers, err := backup.NewStreamingReaderS3(ctx, s3cfg, backup.EncoderTypeASB)
	if err != nil {
		panic(err)
	}

	source := dbNameSource
	target := dbNameTarget
	restoreCfg := backup.NewDefaultRestoreConfig()
	restoreCfg.Parallel = 3
	restoreCfg.Namespace = &backup.RestoreNamespaceConfig{
		Source:      &source,
		Destination: &target,
	}

	// set compression policy
	restoreCfg.CompressionPolicy = &backup.CompressionPolicy{
		Mode: backup.CompressZSTD,
	}

	restoreHandler, err := c.Restore(ctx, restoreCfg, readers)
	if err != nil {
		panic(err)
	}

	// use restoreHandler.Wait() to wait for the job to finish or fail
	err = restoreHandler.Wait(ctx)
	if err != nil {
		log.Printf("Restore failed: %v", err)
	}

	fmt.Println("restore done")
}
