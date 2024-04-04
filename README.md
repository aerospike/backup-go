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
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
)

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClientConfig := backup.NewConfig()

	backupClient, err := backup.NewClient(aerospikeClient, "client_id", slog.Default(), backupClientConfig)
	if err != nil {
		panic(err)
	}

	backupWaitGroup := sync.WaitGroup{}
	// start 5 backup jobs
	for i := 0; i < 5; i++ {
		backupWaitGroup.Add(1)
		go func(i int) {
			defer backupWaitGroup.Done()

			fname := fmt.Sprintf("file%d.abs", i)
			file, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			writers := []io.Writer{file}

			backupCfg := backup.NewBackupConfig()
			backupCfg.Namespace = "test"

			ctx := context.Background()
			handler, err := backupClient.Backup(ctx, writers, backupCfg)
			if err != nil {
				panic(err)
			}

			// optionally check the stats of the backup job
			_ = handler.GetStats()

			// use handler.Wait() to wait for the job to finish or fail
			ctx = context.Background()
			err = handler.Wait(ctx)
			if err != nil {
				log.Printf("Backup %d failed: %v", i, err)
			}
		}(i)
	}

	restoreWaitGroup := sync.WaitGroup{}
	// start 5 restore jobs from the files we backed up
	for i := 0; i < 5; i++ {
		restoreWaitGroup.Add(1)
		go func(i int) {
			defer restoreWaitGroup.Done()

			fname := fmt.Sprintf("file%d.abs", i)
			file, err := os.Open(fname)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			readers := []io.Reader{file}
			restoreCfg := backup.NewRestoreConfig()

			ctx := context.Background()
			handler, err := backupClient.Restore(ctx, readers, restoreCfg)
			if err != nil {
				panic(err)
			}

			// optionally check the stats of the restore job
			_ = handler.GetStats()

			// use handler.Wait() to wait for the job to finish or fail
			ctx = context.Background()
			err = handler.Wait(ctx)
			if err != nil {
				log.Printf("Restore %d failed: %v", i, err)
			}
		}(i)
	}
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