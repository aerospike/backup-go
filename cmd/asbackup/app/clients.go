package app

import (
	"context"
	"fmt"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newAerospikeClient(cfg *client.AerospikeConfig) (*aerospike.Client, error) {
	if len(cfg.Seeds) < 1 {
		return nil, fmt.Errorf("at least one seed must be provided")
	}

	p, err := cfg.NewClientPolicy()
	if err != nil {
		return nil, fmt.Errorf("failed to create new aerospike policy: %w", err)
	}

	asClient, err := aerospike.NewClientWithPolicy(p, cfg.Seeds[0].Host, cfg.Seeds[0].Port)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike asClient: %w", err)
	}

	return asClient, nil
}

func newS3Client(ctx context.Context, a *models.AwsS3) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(a.Profile),
		config.WithRegion(a.Region),
	)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if a.Endpoint != "" {
			o.BaseEndpoint = &a.Endpoint
		}

		o.UsePathStyle = true
	})

	return s3Client, nil
}
