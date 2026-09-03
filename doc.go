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

/*
Package backup provides a library for backing up and restoring Aerospike data.

# Overview

A [Client] wraps an Aerospike client and starts jobs. Every job is asynchronous:
the call returns a handler as soon as the job has started, and the handler is
used to wait for the result, read statistics and read metrics.

There are three operations:

  - [Client.Backup] reads records from a cluster and writes them to a storage
    backend through a [Writer].
  - [Client.Restore] reads a backup through a [StreamingReader] and writes the
    records back into a cluster.
  - [Client.Estimate] samples records to predict the size of a backup, without
    writing anything.

Backups are written in the ASB (Aerospike Backup) text format, implemented by
package [github.com/aerospike/backup-go/io/encoding/asb].

# Getting started

	asClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
	if aerr != nil {
		// handle error
	}

	backupClient, err := backup.NewClient(asClient)
	if err != nil {
		// handle error
	}

	ctx := context.Background()

	writer, err := local.NewWriter(ctx, options.WithDir("backups_folder"))
	if err != nil {
		// handle error
	}

	cfg := backup.NewDefaultBackupConfig()
	cfg.Namespace = "source-ns"

	handler, err := backupClient.Backup(ctx, cfg, writer, nil)
	if err != nil {
		// handle error
	}

	if err := handler.Wait(ctx); err != nil {
		// handle error
	}

A complete, compiling program that backs up and then restores is in
examples/readme/main.go.

# Storage backends

A backup destination is a [Writer] and a backup source is a [StreamingReader].
Implementations live under io/storage and are configured with the shared
functional options from [github.com/aerospike/backup-go/io/storage/options]:

  - io/storage/local for a local directory or file
  - io/storage/aws/s3 for AWS S3 and S3-compatible storage
  - io/storage/gcp/storage for Google Cloud Storage
  - io/storage/azure/blob for Azure Blob Storage
  - io/storage/std for stdin and stdout

# Cancellation

The context passed to [Client.Backup] and [Client.Restore] governs the job
itself; canceling it stops the job. The context passed to the handler's Wait
only governs the waiting, so it may be a different one.

# Stability

This module is at v0.x and does not promise API compatibility between releases.
Packages under pkg/server are under active development and change most often.
*/
package backup
