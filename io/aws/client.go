package aws

import (
	"context"
	"fmt"

	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	s3DefaultChunkSize = 5 * 1024 * 1024                // 5MB, minimum size of a part
	s3maxFile          = s3DefaultChunkSize * 1_000_000 // 5 TB
	s3type             = "s3"
)

func newS3Client(ctx context.Context, awsConfig *models.S3Config) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(awsConfig.Profile),
		config.WithRegion(awsConfig.Region),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to load SDK s3Config, %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if awsConfig.Endpoint != "" {
			o.BaseEndpoint = &awsConfig.Endpoint
		}

		o.UsePathStyle = true
	})

	return client, nil
}
