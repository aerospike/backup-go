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
	maxS3File          = s3DefaultChunkSize * 1_000_000 // 5 TB
	s3type             = "s3"
)

func newS3Client(ctx context.Context, s3Config *models.S3Config) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(s3Config.Profile),
		config.WithRegion(s3Config.Region),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to load SDK s3Config, %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if s3Config.Endpoint != "" {
			o.BaseEndpoint = &s3Config.Endpoint
		}

		o.UsePathStyle = true
	})

	return client, nil
}
