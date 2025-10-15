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
	"fmt"
	"log"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/encoding/asb"
	s3Storasge "github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	dbHost       = "127.0.0.1"
	dbPort       = 3000
	dbUser       = "tester"
	dbPass       = "psw"
	dbNameSource = "source-ns1"
	dbNameTarget = "source-ns2"
	backupFolder = "backup_folder"
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
	s3Client, err := getS3Client(
		ctx,
		"minio",
		"eu",
		"http://localhost:9000",
	)
	if err != nil {
		log.Fatal(err)
	}

	writers, err := s3Storasge.NewWriter(
		ctx,
		s3Client,
		"backup",
		options.WithDir(backupFolder),
		options.WithRemoveFiles(),
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

	// Use backupHandler.Wait(ctx) to wait for the job to finish or fail.
	// You can use different context here, and if it is canceled
	// backupClient.Backup(ctx, backupCfg, writers) context will be cancelled too.
	err = backupHandler.Wait(ctx)
	if err != nil {
		log.Printf("Backup failed: %v", err)
	}

	fmt.Println("backup done")
}

func runRestore(ctx context.Context, c *backup.Client) {
	s3Client, err := getS3Client(
		ctx,
		"minio",
		"eu",
		"http://localhost:9000",
	)
	if err != nil {
		log.Fatal(err)
	}

	reader, err := s3Storasge.NewReader(
		ctx,
		s3Client,
		"backup",
		options.WithDir(backupFolder),
		options.WithValidator(asb.NewValidator()),
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

	restoreHandler, err := c.Restore(ctx, restoreCfg, reader)
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

	fmt.Println("restore done")
}

func getS3Client(ctx context.Context, profile, region, endpoint string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(profile),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}

		o.UsePathStyle = true
	})

	return client, nil
}
