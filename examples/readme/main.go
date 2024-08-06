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
