package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	asModels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/client"
)

type ASBackup struct {
	backupClient *backup.Client
	backupConfig *backup.BackupConfig
	writer       backup.Writer
}

func NewASBackup(
	ctx context.Context,
	clientConfig *client.AerospikeConfig,
	backupParams *models.Backup,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	storage *models.Storage,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	logger *slog.Logger,
) (*ASBackup, error) {
	aerospikeClient, err := newAerospikeClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %v", err)
	}

	backupConfig, err := mapBackupConfig(backupParams)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup config: %v", err)
	}

	backupConfig.CompressionPolicy = mapCompressionPolicy(compression)
	backupConfig.EncryptionPolicy = mapEncryptionPolicy(encryption)
	backupConfig.SecretAgentConfig = mapSecretAgentConfig(secretAgent)
	// TODO: check if we need to pass ID and ScanLimiter?
	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %v", err)
	}

	writer, err := getWriter(ctx, storage, awsS3, gcpStorage, azureBlob)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup writer: %v", err)
	}

	return &ASBackup{
		backupClient: backupClient,
		backupConfig: backupConfig,
		writer:       writer,
	}, nil
}

func getWriter(
	ctx context.Context,
	storage *models.Storage,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) (backup.Writer, error) {
	switch {
	case awsS3.Region != "":
		return newS3Writer(ctx, awsS3, storage)
	case gcpStorage.Host != "":
		return newGcpWriter()
	case azureBlob.Host != "":
		return newAzureWriter()
	default:
		return newLocalWriter(storage)
	}
}

func (b *ASBackup) Run(ctx context.Context) error {
	h, err := b.backupClient.Backup(ctx, b.backupConfig, b.writer)
	if err != nil {
		return fmt.Errorf("failed to start backup: %v", err)
	}

	if err := h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to backup: %v", err)
	}

	printReport(h.GetStats())

	return nil
}

func printReport(stats *asModels.BackupStats) {
	fmt.Println("Done:...", stats)
}
