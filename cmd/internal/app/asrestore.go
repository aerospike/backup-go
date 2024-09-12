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

const idRestore = "asrestore-cli"

type ASRestore struct {
	backupClient  *backup.Client
	restoreConfig *backup.RestoreConfig
	reader        backup.StreamingReader
}

//nolint:dupl // Code is very similar as NewASBackup but different.
func NewASRestore(
	ctx context.Context,
	clientConfig *client.AerospikeConfig,
	restoreParams *models.Restore,
	commonParams *models.Common,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	logger *slog.Logger,
) (*ASRestore, error) {
	aerospikeClient, err := newAerospikeClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	restoreConfig, err := mapRestoreConfig(
		restoreParams,
		commonParams,
		compression,
		encryption,
		secretAgent,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore config: %w", err)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idRestore))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	reader, err := getReader(ctx, restoreParams, commonParams, awsS3, gcpStorage, azureBlob)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup reader: %w", err)
	}

	return &ASRestore{
		backupClient:  backupClient,
		restoreConfig: restoreConfig,
		reader:        reader,
	}, nil
}

func (r *ASRestore) Run(ctx context.Context) error {
	h, err := r.backupClient.Restore(ctx, r.restoreConfig, r.reader)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	if err := h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to restore: %w", err)
	}

	printRestoreReport(h.GetStats())

	return nil
}

func getReader(
	ctx context.Context,
	restoreParams *models.Restore,
	commonParams *models.Common,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) (backup.StreamingReader, error) {
	switch {
	case awsS3.Region != "":
		return newS3Reader(ctx, awsS3, restoreParams, commonParams)
	case gcpStorage.BucketName != "":
		return newGcpReader(ctx, gcpStorage, restoreParams, commonParams)
	case azureBlob.ContainerName != "":
		return newAzureReader(ctx, azureBlob, restoreParams, commonParams)
	default:
		return newLocalReader(restoreParams, commonParams)
	}
}

func printRestoreReport(stats *bModels.RestoreStats) {
	fmt.Println("Restore Report")
	fmt.Println("--------------")
	fmt.Printf("Start Time:           %s\n", stats.StartTime.Format(time.RFC1123))
	fmt.Printf("Duration:             %s\n", stats.GetDuration())
	fmt.Printf("Records Read:         %d\n", stats.GetReadRecords())
	fmt.Printf("sIndex Read:          %d\n", stats.GetSIndexes())
	fmt.Printf("UDFs Read:            %d\n", stats.GetUDFs())
	fmt.Printf("Bytes Written:        %d bytes\n", stats.GetBytesWritten())

	fmt.Printf("Expired Records:      %d\n", stats.GetRecordsExpired())
	fmt.Printf("Skipped Records:      %d\n", stats.GetRecordsSkipped())
	fmt.Printf("Ignored Records:      %d\n", stats.GetRecordsIgnored())
	fmt.Printf("Freasher Records:     %d\n", stats.GetRecordsFresher())
	fmt.Printf("Existed Records:      %d\n", stats.GetRecordsExisted())
	fmt.Printf("Inserted Records:     %d\n", stats.GetRecordsInserted())
	fmt.Printf("Total Bytes Read:     %d\n", stats.GetTotalBytesRead())
}
