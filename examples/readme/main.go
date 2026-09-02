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

// This is the sample referenced from README.md: it backs up a namespace to a
// local directory and restores it back, using the default configuration with
// only a few fields adjusted. Every other option is documented on the
// ConfigBackup and ConfigRestore types.
//
// It expects an Aerospike server on 127.0.0.1:3000 with the "test" namespace.
package main

import (
	"context"
	"log"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
)

const backupDir = "backups_folder"

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		log.Fatal(aerr)
	}
	defer aerospikeClient.Close()

	backupClient, err := backup.NewClient(aerospikeClient)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	runBackup(ctx, backupClient)
	runRestore(ctx, backupClient)
}

func runBackup(ctx context.Context, backupClient *backup.Client) {
	// To back up into a single file use options.WithFile(fileName) instead.
	writer, err := local.NewWriter(
		ctx,
		options.WithRemoveFiles(),
		options.WithDir(backupDir),
	)
	if err != nil {
		log.Fatal(err)
	}

	cfg := backup.NewDefaultBackupConfig()
	// NewDefaultBackupConfig already sets Namespace to "test", but set it
	// explicitly so the namespace being backed up is obvious.
	cfg.Namespace = "test"
	cfg.ParallelRead = 10
	cfg.ParallelWrite = 10

	// Estimate does not write anything; it samples records to predict the size.
	estimate, err := backupClient.Estimate(ctx, cfg, 1000)
	if err != nil {
		log.Printf("Estimate failed: %v", err)
	} else {
		log.Printf("Estimated backup size: %d bytes", estimate)
	}

	// The last argument is a reader, needed only to resume from a state file.
	handler, err := backupClient.Backup(ctx, cfg, writer, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Wait blocks until the job finishes or fails. Its context governs only the
	// waiting; the job itself is bound to the context passed to Backup, so
	// canceling that one stops the job.
	if err := handler.Wait(ctx); err != nil {
		log.Fatalf("Backup failed: %v", err)
	}

	stats := handler.GetStats()
	log.Printf("Backed up %d records into %d file(s), %d bytes",
		stats.GetReadRecords(), stats.GetFileCount(), stats.GetBytesWritten())
}

func runRestore(ctx context.Context, backupClient *backup.Client) {
	// To restore from a single file use options.WithFile(fileName) instead.
	// The validator makes the reader skip files that are not ASB backups.
	reader, err := local.NewReader(
		ctx,
		options.WithDir(backupDir),
		options.WithValidator(asb.NewValidator()),
	)
	if err != nil {
		log.Fatal(err)
	}

	cfg := backup.NewDefaultRestoreConfig()
	cfg.Parallel = 5

	handler, err := backupClient.Restore(ctx, cfg, reader)
	if err != nil {
		log.Fatal(err)
	}

	if err := handler.Wait(ctx); err != nil {
		log.Fatalf("Restore failed: %v", err)
	}

	stats := handler.GetStats()
	log.Printf("Restored %d records, %d skipped, %d expired",
		stats.GetRecordsInserted(), stats.GetRecordsSkipped(), stats.GetRecordsExpired())
}
