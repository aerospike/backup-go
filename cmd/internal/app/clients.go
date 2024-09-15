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

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
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
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if a.Endpoint != "" {
			o.BaseEndpoint = &a.Endpoint
		}

		o.UsePathStyle = true
	})

	return s3Client, nil
}

func newGcpClient(ctx context.Context, g *models.GcpStorage) (*storage.Client, error) {
	opts := make([]option.ClientOption, 0)

	if g.KeyFile != "" {
		opts = append(opts, option.WithCredentialsFile(g.KeyFile))
	}

	if g.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(g.Endpoint), option.WithoutAuthentication())
	}

	gcpClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcp client: %w", err)
	}

	return gcpClient, nil
}

func newAzureClient(a *models.AzureBlob) (*azblob.Client, error) {
	var (
		azClient *azblob.Client
		err      error
	)

	switch {
	case a.AccountName != "" && a.AccountKey != "":
		cred, err := azblob.NewSharedKeyCredential(a.AccountName, a.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create azure sahred key credentials: %w", err)
		}

		azClient, err = azblob.NewClientWithSharedKeyCredential(a.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with shared key: %w", err)
		}
	case a.TenantID != "" && a.ClientID != "" && a.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(a.TenantID, a.ClientID, a.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create azure AAD credentials: %w", err)
		}

		azClient, err = azblob.NewClient(a.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with AAD: %w", err)
		}
	default:
		azClient, err = azblob.NewClientWithNoCredential(a.Endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with SAS: %w", err)
		}
	}

	return azClient, nil
}
