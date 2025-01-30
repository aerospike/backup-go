# backup-go
[![Tests](https://github.com/aerospike/backup-go/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/backup-go/actions/workflows/tests.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/backup-go)](https://pkg.go.dev/github.com/aerospike/backup-go)
[![codecov](https://codecov.io/gh/aerospike/backup-go/graph/badge.svg?token=S0gfl2zCcZ)](https://codecov.io/gh/aerospike/backup-go)

A Go library for backing up and restoring [Aerospike](https://aerospike.com/) data.

## Design

This Aerospike backup package is built around the [Aerospike Go client](https://github.com/aerospike/aerospike-client-go). The package uses a client structure to start backup and restore jobs. The client structure is thread safe, backup and restore jobs can be started in multiple goroutines. When the client is used to start backup and restore jobs, a handler is immediately returned that is used to check the job's status, errors, and wait for it to finish. Here is how to use the package at a high level.

- Wrap Aerospike Go clients with the backup package Client object.
- Start backup and restore jobs using that backup client. These client methods return a handler which is used to monitor the started job. Started jobs run in parallel.
- Use the returned handlers to monitor the started jobs.

## Usage

The following is a simple example using a backup client to start backup and restore jobs. Errors should be properly handled in production code.

```Go
package main

import (
	"context"
	"log"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/local"
)

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithID("client_id"))
	if err != nil {
		panic(err)
	}

	// For backup to single file use local.WithFile(fileName)
	writers, err := local.NewWriter(
		context.Background(),
		local.WithRemoveFiles(),
		local.WithDir("backups_folder"),
	)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = "test"
	backupCfg.ParallelRead = 10
	backupCfg.ParallelWrite = 10
	ctx := context.Background()

	backupHandler, err := backupClient.Backup(ctx, backupCfg, writers, nil)
	if err != nil {
		panic(err)
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
		local.WithValidator(asb.NewValidator()),
		local.WithDir("backups_folder"),
	)
	if err != nil {
		panic(err)
	}

	restoreHandler, err := backupClient.Restore(ctx, restoreCfg, reader)
	if err != nil {
		panic(err)
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
```
More examples can be found under the [examples](examples) folder.

## Prerequisites

Requirements

- [Go](https://go.dev/) version v1.23+
- [Aerospike Go client](https://github.com/aerospike/aerospike-client-go) v7

Testing Requirements

- [Mockery](https://github.com/vektra/mockery) to generate test mocks

## Installation

1. Install requirements.
2. Use `go get https://github.com/aerospike/backup-go`

## License

The Aerospike Backup package is made available under the terms of the Apache License, Version 2, as stated in the file LICENSE.

Individual files may be made available under their own specific license, all compatible with Apache License, Version 2. Please see individual files for details.
