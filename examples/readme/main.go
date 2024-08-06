package main

import (
	"context"
	"log"
	"log/slog"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
)

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backup.NewClient(aerospikeClient, "client_id", slog.Default())
	if err != nil {
		panic(err)
	}

	writers, err := backup.NewWriterLocal("backups_folder", false)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = "test"
	backupCfg.Parallel = 5
	ctx := context.Background()

	backupHandler, err := backupClient.Backup(ctx, backupCfg, writers)
	if err != nil {
		panic(err)
	}

	// use backupHandler.Wait() to wait for the job to finish or fail
	err = backupHandler.Wait(ctx)
	if err != nil {
		log.Printf("Backup failed: %v", err)
	}

	restoreCfg := backup.NewDefaultRestoreConfig()
	restoreCfg.Parallel = 5

	streamingReader, err := backup.NewStreamingReaderLocal("backups_folder", backup.EncoderTypeASB)
	if err != nil {
		panic(err)
	}

	restoreHandler, err := backupClient.Restore(ctx, restoreCfg, streamingReader)
	if err != nil {
		panic(err)
	}

	// use restoreHandler.Wait() to wait for the job to finish or fail
	err = restoreHandler.Wait(ctx)
	if err != nil {
		log.Printf("Restore failed: %v", err)
	}

	// optionally check the stats of the restore job
	_ = restoreHandler.GetStats()
}
