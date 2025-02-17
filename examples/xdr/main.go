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

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/local"
)

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithID("client_id"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// For backup to single file use local.WithFile(fileName)
	writers, err := local.NewWriter(
		ctx,
		ioStorage.WithRemoveFiles(),
		ioStorage.WithDir("backups_folder"),
	)
	if err != nil {
		log.Fatal(err)
	}

	xdrConfig := &backup.ConfigBackupXDR{
		DC:             "dc1",
		LocalAddress:   "host.docker.internal",
		LocalPort:      3000,
		Namespace:      "test",
		ParallelWrite:  10,
		Rewind:         "all",
		MaxConnections: 10,
	}

	backupHandler, err := backupClient.BackupXDR(ctx, xdrConfig, writers)
	if err != nil {
		log.Fatal(err)
	}

	// Use backupHandler.Wait(ctx) to wait for the job to finish or fail.
	// You can use different context here, and if it is canceled
	// backupClient.Backup(ctx, backupCfg, writers) context will be cancelled too.
	err = backupHandler.Wait(ctx)
	if err != nil {
		log.Printf("Backup failed: %v", err)
	}

	restoreCfg := backup.NewDefaultRestoreConfig()
	restoreCfg.Parallel = 5

	// For restore from single file use local.WithFile(fileName)
	reader, err := local.NewReader(
		ctx,
		ioStorage.WithDir("backups_folder"),
		ioStorage.WithValidator(asb.NewValidator()),
		ioStorage.WithSorting(), // Required for ASBX files
	)
	if err != nil {
		log.Fatal(err)
	}

	restoreHandler, err := backupClient.Restore(ctx, restoreCfg, reader)
	if err != nil {
		log.Fatal(err)
	}

	// Use restoreHandler.Wait(ctx) to wait for the job to finish or fail.
	// You can use different context here, and if it is canceled
	// backupClient.Restore(ctx, restoreCfg, streamingReader) context will be cancelled too.
	err = restoreHandler.Wait(ctx)
	if err != nil {
		log.Printf("Restore failed: %v", err)
	}

	// optionally check the stats of the restore job
	_ = restoreHandler.GetStats()
}
