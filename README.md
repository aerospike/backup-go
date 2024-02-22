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
package backuplib

import (
	"io"
	"os"

	"github.com/aerospike/aerospike-client-go/v7"
    "github.com/aerospike/aerospike-tools-backup-lib"
)

func main() {
	aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backuplib.NewClient(aerospikeClient, Config{})
	if err != nil {
		panic(err)
	}

	backupHandlers := make()

    // start 5 backup jobs
	for i := 0; i < 5; i++ {
		fname := "file" + string(i)
		file, err := os.Open(fname)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		backupCfg := backuplib.NewBackupToWriterConfig()
		backupCfg.Parallel = 4
        backupCfg.Namespace = "test"

		writers := []io.Writer{file}

		handler, err := backupClient.BackupToWriter(writers, backupCfg)
		if err != nil {
			panic(err)
		}
        // optionally check the status of the backup job
		stats := handler.GetStatus()
		// use handler.Wait() to wait for the job to finish or fail
		err = handler.Wait()
		if err != nil {
			panic(err)
		}
	}

	err = <-backupErrors
	if err != nil {
		panic(err)
	}

	restoreErrors := make(chan error, 5)

	// start 5 restore jobs from the files we backed up

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