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
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

const maxRack = 1000000

func newAerospikeClient(
	cfg *client.AerospikeConfig,
	cp *models.ClientPolicy,
	racks string,
	warmUp int,
) (*aerospike.Client, error) {
	if len(cfg.Seeds) < 1 {
		return nil, fmt.Errorf("at least one seed must be provided")
	}

	p, err := cfg.NewClientPolicy()
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client policy: %w", err)
	}

	p.Timeout = time.Duration(cp.Timeout) * time.Millisecond
	p.IdleTimeout = time.Duration(cp.IdleTimeout) * time.Millisecond
	p.LoginTimeout = time.Duration(cp.LoginTimeout) * time.Millisecond

	if racks != "" {
		racksIDs, err := parseRacks(racks)
		if err != nil {
			return nil, err
		}

		p.RackIds = racksIDs
		p.RackAware = true
	}

	asClient, err := aerospike.NewClientWithPolicyAndHost(p, toHosts(cfg.Seeds)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client: %w", err)
	}

	if warmUp > 0 {
		_, err = asClient.WarmUp(warmUp)
		if err != nil {
			return nil, fmt.Errorf("failed to warm up Aerospike client: %w", err)
		}
	}

	return asClient, nil
}

func newS3Client(ctx context.Context, a *models.AwsS3) (*s3.Client, error) {
	cfgOpts := make([]func(*config.LoadOptions) error, 0)

	if a.Profile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(a.Profile))
	}

	if a.Region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(a.Region))
	}

	if a.AccessKeyID != "" && a.SecretAccessKey != "" {
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: a.AccessKeyID, SecretAccessKey: a.SecretAccessKey,
			},
		}))
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
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
		return nil, fmt.Errorf("failed to create GCP client: %w", err)
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
			return nil, fmt.Errorf("failed to create Azure shared key credentials: %w", err)
		}

		azClient, err = azblob.NewClientWithSharedKeyCredential(a.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with shared key: %w", err)
		}
	case a.TenantID != "" && a.ClientID != "" && a.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(a.TenantID, a.ClientID, a.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure AAD credentials: %w", err)
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

func toHosts(htpSlice client.HostTLSPortSlice) []*aerospike.Host {
	hosts := make([]*aerospike.Host, len(htpSlice))
	for i, htp := range htpSlice {
		hosts[i] = &aerospike.Host{
			Name:    htp.Host,
			TLSName: htp.TLSName,
			Port:    htp.Port,
		}
	}

	return hosts
}

func parseRacks(racks string) ([]int, error) {
	racksStringSlice := splitByComma(racks)
	racksIntSlice := make([]int, 0, len(racksStringSlice))

	for i := range racksStringSlice {
		rackID, err := strconv.Atoi(racksStringSlice[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse racks: %w", err)
		}

		if rackID < 0 {
			return nil, fmt.Errorf("rack id %d invalid, should be positive number", rackID)
		}

		if rackID > maxRack {
			return nil, fmt.Errorf("rack id %d invalid, should not exceed %d", rackID, maxRack)
		}

		racksIntSlice = append(racksIntSlice, rackID)
	}

	return racksIntSlice, nil
}
