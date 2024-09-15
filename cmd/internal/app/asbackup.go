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

package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	bModels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/client"
)

const idBackup = "asbackup-cli"

type ASBackup struct {
	backupClient *backup.Client
	backupConfig *backup.BackupConfig
	writer       backup.Writer
}

//nolint:dupl // Code is very similar as NewASRestore but different.
func NewASBackup(
	ctx context.Context,
	clientConfig *client.AerospikeConfig,
	backupParams *models.Backup,
	commonParams *models.Common,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	logger *slog.Logger,
) (*ASBackup, error) {
	if err := validateStorages(awsS3, gcpStorage, azureBlob); err != nil {
		return nil, err
	}

	aerospikeClient, err := newAerospikeClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	backupConfig, err := mapBackupConfig(
		backupParams,
		commonParams,
		compression,
		encryption,
		secretAgent,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup config: %w", err)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idBackup))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	writer, err := getWriter(ctx, backupParams, commonParams, awsS3, gcpStorage, azureBlob)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup writer: %w", err)
	}

	return &ASBackup{
		backupClient: backupClient,
		backupConfig: backupConfig,
		writer:       writer,
	}, nil
}

func (b *ASBackup) Run(ctx context.Context) error {
	h, err := b.backupClient.Backup(ctx, b.backupConfig, b.writer)
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	if err := h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to backup: %w", err)
	}

	printBackupReport(h.GetStats())

	return nil
}

func getWriter(
	ctx context.Context,
	backupParams *models.Backup,
	commonParams *models.Common,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) (backup.Writer, error) {
	switch {
	case awsS3.Region != "":
		return newS3Writer(ctx, awsS3, backupParams, commonParams)
	case gcpStorage.BucketName != "":
		return newGcpWriter(ctx, gcpStorage, backupParams, commonParams)
	case azureBlob.ContainerName != "":
		return newAzureWriter(ctx, azureBlob, backupParams, commonParams)
	default:
		return newLocalWriter(backupParams, commonParams)
	}
}

func printBackupReport(stats *bModels.BackupStats) {
	fmt.Println("Backup Report")
	fmt.Println("--------------")
	fmt.Printf("Start Time:           %s\n", stats.StartTime.Format(time.RFC1123))
	fmt.Printf("Duration:             %s\n", stats.GetDuration())
	fmt.Printf("Records Read:         %d\n", stats.GetReadRecords())
	fmt.Printf("sIndex Read:          %d\n", stats.GetSIndexes())
	fmt.Printf("UDFs Read:            %d\n", stats.GetUDFs())
	fmt.Printf("Bytes Written:        %d bytes\n", stats.GetBytesWritten())

	fmt.Printf("Total Records:        %d\n", stats.TotalRecords)
	fmt.Printf("Files Written:        %d\n", stats.GetFileCount())
}
