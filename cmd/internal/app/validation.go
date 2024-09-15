package app

import (
	"fmt"

	"github.com/aerospike/backup-go/cmd/internal/models"
)

func validateStorages(
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) error {
	var count int

	if awsS3.Region != "" || awsS3.Profile != "" || awsS3.Endpoint != "" {
		count++
	}

	if gcpStorage.BucketName != "" || gcpStorage.KeyFile != "" || gcpStorage.Endpoint != "" {
		count++
	}

	if azureBlob.ContainerName != "" || azureBlob.AccountName != "" || azureBlob.AccountKey != "" ||
		azureBlob.Endpoint != "" || azureBlob.TenantID != "" || azureBlob.ClientID != "" ||
		azureBlob.ClientSecret != "" {
		count++
	}

	if count > 1 {
		return fmt.Errorf("only one cloud provider can be configured")
	}

	return nil
}
