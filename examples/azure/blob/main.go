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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	ioStorage "github.com/aerospike/backup-go/io/storage/options"
)

const (
	dbHost        = "127.0.0.1"
	dbPort        = 3000
	dbUser        = "tester"
	dbPass        = "psw"
	dbNameSource  = "source-ns1"
	dbNameTarget  = "source-ns2"
	containerName = "test-container"
	folderName    = "backups_folder"

	azureAccountName = "devstoreaccount1"
	azureAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azureAddress     = "http://127.0.0.1:10001/devstoreaccount1"
)

func main() {
	ctx := context.Background()
	backupClient := initBackupClient()

	runBackup(ctx, backupClient)

	runRestore(ctx, backupClient)
}

func initBackupClient() *backup.Client {
	policy := aerospike.NewClientPolicy()
	policy.User = dbUser
	policy.Password = dbPass

	aerospikeClient, aerr := aerospike.NewClientWithPolicy(
		policy,
		dbHost,
		dbPort,
	)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithID("client_id"))
	if err != nil {
		log.Fatal(err)
	}

	return backupClient
}

func runBackup(ctx context.Context, c *backup.Client) {
	cred, err := azblob.NewSharedKeyCredential(azureAccountName, azureAccountKey)
	if err != nil {
		log.Fatal(err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(azureAddress, cred, nil)
	if err != nil {
		log.Fatal(err)
	}

	// For backup to single file use gcpStorage.WithFile(fileName)
	writers, err := blob.NewWriter(
		ctx,
		client,
		containerName,
		ioStorage.WithDir(folderName),
		ioStorage.WithRemoveFiles(),
	)
	if err != nil {
		log.Fatal(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = dbNameSource
	backupCfg.ParallelRead = 10

	// set compression policy
	backupCfg.CompressionPolicy = backup.NewCompressionPolicy(backup.CompressZSTD, 20)

	backupHandler, err := c.Backup(ctx, backupCfg, writers, nil)
	if err != nil {
		log.Fatal(err)
	}

	// use backupHandler.Wait() to wait for the job to finish or fail.
	// You can use different context here, and if it is canceled
	// backupClient.Backup(ctx, backupCfg, writers) context will be cancelled too.
	err = backupHandler.Wait(ctx)
	if err != nil {
		log.Printf("Backup failed: %v", err)
	}

	log.Println("backup done:", backupHandler.GetStats().ReadRecords.Load())
}

func runRestore(ctx context.Context, c *backup.Client) {
	cred, err := azblob.NewSharedKeyCredential(azureAccountName, azureAccountKey)
	if err != nil {
		log.Fatal(err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(azureAddress, cred, nil)
	if err != nil {
		log.Fatal(err)
	}

	// For restore from single file use gcpStorage.WithFile(fileName)
	readers, err := blob.NewReader(
		ctx,
		client,
		containerName,
		ioStorage.WithDir(folderName),
		ioStorage.WithValidator(asb.NewValidator()),
	)
	if err != nil {
		log.Fatal(err)
	}

	source := dbNameSource
	target := dbNameTarget
	restoreCfg := backup.NewDefaultRestoreConfig()
	restoreCfg.Parallel = 3
	restoreCfg.Namespace = &backup.RestoreNamespaceConfig{
		Source:      &source,
		Destination: &target,
	}

	// set compression policy
	restoreCfg.CompressionPolicy = backup.NewCompressionPolicy(backup.CompressZSTD, 20)

	restoreHandler, err := c.Restore(ctx, restoreCfg, readers)
	if err != nil {
		log.Fatal(err)
	}

	// use restoreHandler.Wait() to wait for the job to finish or fail.
	// You can use different context here, and if it is canceled
	// backupClient.Restore(ctx, restoreCfg, streamingReader) context will be cancelled too.
	err = restoreHandler.Wait(ctx)
	if err != nil {
		log.Printf("Restore failed: %v", err)
	}

	log.Println("restore done:", restoreHandler.GetStats().ReadRecords.Load())
}
