# aerospike-tools-backup-lib

A library for backing up and restoring Aerospike data.

### Design

This Aerospike backup package is built around the (Aerospike Go client)[https://github.com/aerospike/aerospike-client-go] . The package uses a client structure to start backup and restore jobs. The client structure is thread safe, backup and restore jobs can be started by multiple threads. When the client is used to start backup and restore jobs, a handler is immediately returned that is used to check the job's status, errors, and wait for it to finish. Here is how to use the package at a high level.

- Wrap Aerospike Go clients with the backup package Client object.
- Start backup and restore jobs using that backup client. These client methods return a handler which is used to monitor the started job. Started jobs run in parallel.
- Use the returned handlers to monitor the started jobs.

### Usage

The following is a simple example using a backup client to start backup and restore jobs. Errors should be properly handled in production code.
```Go
package main

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/aerospike/aerospike-client-go/v7"
	backuplib "github.com/aerospike/aerospike-tools-backup-lib"
)

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClientConfig := backuplib.NewConfig()

	backupClient, err := backuplib.NewClient(aerospikeClient, backupClientConfig)
	if err != nil {
		panic(err)
	}

	backupWaitGroup := sync.WaitGroup{}
	// start 5 backup jobs
	for i := 0; i < 5; i++ {
		backupWaitGroup.Add(1)
		go func(i int) {
			defer backupWaitGroup.Done()

			fname := "file" + string(i)
			file, err := os.Open(fname)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			writers := []io.Writer{file}
			backupCfg := backuplib.NewBackupToWriterConfig()
			backupCfg.Namespace = "test"
			handler, err := backupClient.BackupToWriter(writers, backupCfg)
			if err != nil {
				panic(err)
			}
			// optionally check the stats of the backup job
			_ = handler.GetStats()
			// use handler.Wait() to wait for the job to finish or fail
			err = handler.Wait()
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

			fname := "file" + string(i)
			file, err := os.Open(fname)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			readers := []io.Reader{file}
			restoreCfg := backuplib.NewRestoreFromReaderConfig()
			handler, err := backupClient.RestoreFromReader(readers, restoreCfg)
			if err != nil {
				panic(err)
			}
			// optionally check the stats of the restore job
			_ = handler.GetStats()
			// use handler.Wait() to wait for the job to finish or fail
			err = handler.Wait()
			if err != nil {
				log.Printf("Restore %d failed: %v", i, err)
			}
		}(i)
	}
}
```
### Prerequisites

Requirements

- (Go)[https://go.dev/] version v1.21+
- (Aerospike Go client v7)[https://github.com/aerospike/aerospike-client-go]

Testing Requirements

- (Only if you need to re-generate test mocks) (mockery)[https://github.com/vektra/mockery]

### Installation

1. Install requirements.
2. Use `go get https://github.com/aerospike/aerospike-tools-backup-lib`

### License

The Aerospike Backup package is made available under the terms of the Apache License, Version 2, as stated in the file LICENSE.

Individual files may be made available under their own specific license, all compatible with Apache License, Version 2. Please see individual files for details.