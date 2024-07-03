# backup-go

A library for backing up and restoring Aerospike data.

### Design

This Aerospike backup package is built around the [Aerospike Go client](https://github.com/aerospike/aerospike-client-go). The package uses a client structure to start backup and restore jobs. The client structure is thread safe, backup and restore jobs can be started by multiple threads. When the client is used to start backup and restore jobs, a handler is immediately returned that is used to check the job's status, errors, and wait for it to finish. Here is how to use the package at a high level.

- Wrap Aerospike Go clients with the backup package Client object.
- Start backup and restore jobs using that backup client. These client methods return a handler which is used to monitor the started job. Started jobs run in parallel.
- Use the returned handlers to monitor the started jobs.

### Usage

The following is a simple example using a backup client to start backup and restore jobs. Errors should be properly handled in production code.

```Go
package main

import (
	"context"
	"log"
	"log/slog"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/io/local"
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

	writers, err := local.NewDirectoryWriterFactory("backups_folder", false)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewBackupConfig()
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

	restoreCfg := backup.NewRestoreConfig()
	restoreCfg.Parallel = 5

	readers, err := local.NewDirectoryReaderFactory("backup_folder", asb.NewASBDecoderFactory())
	if err != nil {
		panic(err)
	}

	restoreHandler, err := backupClient.Restore(ctx, restoreCfg, readers)
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

```

### Prerequisites

Requirements

- [Go](https://go.dev/) version v1.21+
- [Aerospike Go client](https://github.com/aerospike/aerospike-client-go) v7

Testing Requirements

- [Mockery](https://github.com/vektra/mockery) to generate test mocks

### Installation

1. Install requirements.
2. Use `go get https://github.com/aerospike/backup-go`

### License

The Aerospike Backup package is made available under the terms of the Apache License, Version 2, as stated in the file LICENSE.

Individual files may be made available under their own specific license, all compatible with Apache License, Version 2. Please see individual files for details.