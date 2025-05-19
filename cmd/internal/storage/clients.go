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

package storage

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	gcpStorage "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aerospike/aerospike-client-go/v8"
	appConfig "github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

// NewAerospikeClient initializes and returns a new Aerospike client with the specified configuration and settings.
// It validates input parameters, applies client policies, and optionally warms up the client for better performance.
// Returns an Aerospike client instance or an error if initialization fails.
func NewAerospikeClient(
	cfg *client.AerospikeConfig,
	cp *models.ClientPolicy,
	racks string,
	warmUp int,
	logger *slog.Logger,
) (*aerospike.Client, error) {
	if len(cfg.Seeds) < 1 {
		return nil, fmt.Errorf("at least one seed must be provided")
	}

	logger.Info("initializing Aerospike client",
		slog.String("seeds", cfg.Seeds.String()),
	)

	p, err := cfg.NewClientPolicy()
	if err != nil {
		return nil, fmt.Errorf("failed to create Aerospike client policy: %w", err)
	}

	p.Timeout = time.Duration(cp.Timeout) * time.Millisecond
	p.IdleTimeout = time.Duration(cp.IdleTimeout) * time.Millisecond
	p.LoginTimeout = time.Duration(cp.LoginTimeout) * time.Millisecond

	if racks != "" {
		racksIDs, err := appConfig.ParseRacks(racks)
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

	// use an adaptive mode for more aggressive retries
	cfgOpts = append(cfgOpts,
		config.WithRetryer(func() aws.Retryer {
			return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
				o.StandardOptions = append(o.StandardOptions,
					func(so *retry.StandardOptions) {
						so.MaxAttempts = a.RetryMaxAttempts
						so.MaxBackoff = time.Duration(a.RetryMaxBackoffSeconds) * time.Second
						so.Backoff = retry.NewExponentialJitterBackoff(
							time.Duration(a.RetryBackoffSeconds) * time.Second,
						)
					})
			})
		}),
	)

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
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return s3Client, nil
}

func newGcpClient(ctx context.Context, g *models.GcpStorage) (*gcpStorage.Client, error) {
	opts := make([]option.ClientOption, 0)

	if g.KeyFile != "" {
		opts = append(opts, option.WithCredentialsFile(g.KeyFile))
	}

	if g.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(g.Endpoint), option.WithoutAuthentication())
	}

	gcpClient, err := gcpStorage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP client: %w", err)
	}

	backoff := gax.Backoff{
		Initial:    time.Duration(g.RetryBackoffInitSeconds) * time.Second,
		Max:        time.Duration(g.RetryBackoffMaxSeconds) * time.Second,
		Multiplier: g.RetryBackoffMultiplier,
	}

	gcpClient.SetRetry(
		gcpStorage.WithPolicy(gcpStorage.RetryAlways),
		gcpStorage.WithBackoff(backoff),
		gcpStorage.WithMaxAttempts(g.RetryMaxAttempts))

	return gcpClient, nil
}

func newAzureClient(a *models.AzureBlob) (*azblob.Client, error) {
	var (
		azClient *azblob.Client
		err      error
	)

	azOpts := &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    int32(a.RetryMaxAttempts),
				TryTimeout:    time.Duration(a.RetryTryTimeoutSeconds) * time.Second,
				RetryDelay:    time.Duration(a.RetryDelaySeconds) * time.Second,
				MaxRetryDelay: time.Duration(a.RetryMaxDelaySeconds) * time.Second,
				StatusCodes: []int{
					http.StatusRequestTimeout,
					http.StatusTooManyRequests,
					http.StatusInternalServerError,
					http.StatusBadGateway,
					http.StatusServiceUnavailable,
					http.StatusGatewayTimeout,
				},
			},
		},
	}

	switch {
	case a.AccountName != "" && a.AccountKey != "":
		cred, err := azblob.NewSharedKeyCredential(a.AccountName, a.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure shared key credentials: %w", err)
		}

		azClient, err = azblob.NewClientWithSharedKeyCredential(a.Endpoint, cred, azOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with shared key: %w", err)
		}
	case a.TenantID != "" && a.ClientID != "" && a.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(a.TenantID, a.ClientID, a.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure AAD credentials: %w", err)
		}

		azClient, err = azblob.NewClient(a.Endpoint, cred, azOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with AAD: %w", err)
		}
	default:
		azClient, err = azblob.NewClientWithNoCredential(a.Endpoint, azOpts)
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
