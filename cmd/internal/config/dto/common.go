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

package dto

import (
	"fmt"
	"strings"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/aerospike/tools-common-go/flags"
)

// App represents the application-level configuration parsed from a YAML file.
type App struct {
	Verbose  bool   `yaml:"verbose"`
	LogLevel string `yaml:"log-level"`
	LogJSON  bool   `yaml:"log-json"`
}

func (a *App) ToModelApp() *models.App {
	return &models.App{
		Verbose:  a.Verbose,
		LogLevel: a.LogLevel,
		LogJSON:  a.LogJSON,
	}
}

// Cluster defines the configuration for connecting to an Aerospike cluster, including seeds, auth, and TLS settings
// parsed from a YAML file.
type Cluster struct {
	Seeds []struct {
		Host    string `yaml:"host"`
		TLSName string `yaml:"tls-name"`
		Port    int    `yaml:"port"`
	} `yaml:"seeds"`
	User               string `yaml:"user"`
	Password           string `yaml:"password"`
	Auth               string `yaml:"auth"`
	ClientTimeout      int64  `yaml:"client-timeout"`
	ClientIdleTimeout  int64  `yaml:"client-idle-timeout"`
	ClientLoginTimeout int64  `yaml:"client-login-timeout"`
	TLS                struct {
		Enable          bool   `yaml:"enable"`
		Name            string `yaml:"name"`
		Protocols       string `yaml:"protocols"`
		CaFile          string `yaml:"ca-file"`
		CaPath          string `yaml:"ca-path"`
		CertFile        string `yaml:"cert-file"`
		KeyFile         string `yaml:"key-file"`
		KeyFilePassword string `yaml:"key-file-password"`
	} `yaml:"tls"`
}

//nolint:gocyclo // This is a long mapping function, no need to brake it into small ones.
func (c *Cluster) ToAerospikeConfig() (*client.AerospikeConfig, error) {
	var f flags.AerospikeFlags

	hosts := make([]string, 0, len(c.Seeds))

	for i := range c.Seeds {
		hostStr := c.Seeds[i].Host
		if c.Seeds[i].TLSName != "" {
			hostStr = fmt.Sprintf("%s:%s", hostStr, c.Seeds[i].TLSName)
		}

		if c.Seeds[i].Port != 0 {
			hostStr = fmt.Sprintf("%s:%v", hostStr, c.Seeds[i].Port)
		}

		hosts = append(hosts, hostStr)
	}

	hostPorts := strings.Join(hosts, ",")

	if hostPorts != "" {
		var seeds flags.HostTLSPortSliceFlag
		if err := seeds.Set(hostPorts); err != nil {
			return nil, fmt.Errorf("failed to set seeds: %w", err)
		}

		f.Seeds = seeds
	}

	if c.User != "" {
		f.User = c.User
	}

	if c.Password != "" {
		var psw flags.PasswordFlag
		if err := psw.Set(c.Password); err != nil {
			return nil, fmt.Errorf("failed to set password: %w", err)
		}

		f.Password = psw
	}

	if c.Auth != "" {
		var authMode flags.AuthModeFlag
		if err := authMode.Set(c.Auth); err != nil {
			return nil, fmt.Errorf("failed to set auth mode: %w", err)
		}

		f.AuthMode = authMode
	}

	f.TLSEnable = c.TLS.Enable
	f.TLSName = c.TLS.Name

	if c.TLS.Protocols != "" {
		var tlsProtocols flags.TLSProtocolsFlag
		if err := tlsProtocols.Set(c.TLS.Protocols); err != nil {
			return nil, fmt.Errorf("failed to set tls protocols: %w", err)
		}

		f.TLSProtocols = tlsProtocols
	}

	if c.TLS.CaFile != "" {
		var tlsRootCaFile flags.CertFlag

		if err := tlsRootCaFile.Set(c.TLS.CaFile); err != nil {
			return nil, fmt.Errorf("failed to set tls root ca file: %w", err)
		}

		f.TLSRootCAFile = tlsRootCaFile
	}

	if c.TLS.CaPath != "" {
		var tlsRootCaPath flags.CertPathFlag

		if err := tlsRootCaPath.Set(c.TLS.CaPath); err != nil {
			return nil, fmt.Errorf("failed to set tls root ca path: %w", err)
		}

		f.TLSRootCAPath = tlsRootCaPath
	}

	if c.TLS.CertFile != "" {
		var tlsCertFile flags.CertFlag

		if err := tlsCertFile.Set(c.TLS.CertFile); err != nil {
			return nil, fmt.Errorf("failed to set tls cert file: %w", err)
		}

		f.TLSCertFile = tlsCertFile
	}

	if c.TLS.KeyFile != "" {
		var tlsKeyFile flags.CertFlag
		if err := tlsKeyFile.Set(c.TLS.KeyFile); err != nil {
			return nil, fmt.Errorf("failed to set tls key file: %w", err)
		}
	}

	if c.TLS.KeyFilePassword != "" {
		var tlsKeyFilePass flags.PasswordFlag
		if err := tlsKeyFilePass.Set(c.TLS.KeyFilePassword); err != nil {
			return nil, fmt.Errorf("failed to set tls key file password: %w", err)
		}
	}

	return f.NewAerospikeConfig(), nil
}

func (c *Cluster) ToModelClientPolicy() *models.ClientPolicy {
	return &models.ClientPolicy{
		Timeout:      c.ClientTimeout,
		IdleTimeout:  c.ClientIdleTimeout,
		LoginTimeout: c.ClientLoginTimeout,
	}
}

// Compression represents the configuration for data compression, including the mode and compression level
// parsed from a YAML file.
type Compression struct {
	Mode  string `yaml:"mode"`
	Level int    `yaml:"level"`
}

func (c *Compression) ToModelCompression() *models.Compression {
	return &models.Compression{
		Mode:  c.Mode,
		Level: c.Level,
	}
}

// Encryption defines encryption configuration options parsed from a YAML file.
// It includes fields for mode, key file, key environment variable, and key secret
// parsed from a YAML file.
type Encryption struct {
	Mode      string `yaml:"mode"`
	KeyFile   string `yaml:"key-file"`
	KeyEnv    string `yaml:"key-env"`
	KeySecret string `yaml:"key-secret"`
}

func (e *Encryption) ToModelEncryption() *models.Encryption {
	return &models.Encryption{
		Mode:      e.Mode,
		KeyFile:   e.KeyFile,
		KeyEnv:    e.KeyEnv,
		KeySecret: e.KeySecret,
	}
}

// SecretAgent defines connection properties for a secure agent, including address, port,
// timeout, and encryption settings parsed from a YAML file.
type SecretAgent struct {
	ConnectionType     string `yaml:"connection-type"`
	Address            string `yaml:"address"`
	Port               int    `yaml:"port"`
	TimeoutMillisecond int    `yaml:"timeout-millisecond"`
	CaFile             string `yaml:"ca-file"`
	IsBase64           bool   `yaml:"is-base64"`
}

func (s *SecretAgent) ToModelSecretAgent() *models.SecretAgent {
	return &models.SecretAgent{
		ConnectionType:     s.ConnectionType,
		Address:            s.Address,
		Port:               s.Port,
		TimeoutMillisecond: s.TimeoutMillisecond,
		CaFile:             s.CaFile,
		IsBase64:           s.IsBase64,
	}
}

// AwsS3 defines configuration for AWS S3 storage including bucket details and retry mechanisms
// parsed from a YAML file.
type AwsS3 struct {
	BucketName       string `yaml:"bucket-name"`
	Region           string `yaml:"region"`
	Profile          string `yaml:"profile"`
	EndpointOverride string `yaml:"endpoint-override"`
	AccessKeyID      string `yaml:"access-key-id"`
	SecretAccessKey  string `yaml:"secret-access-key"`
	StorageClass     string `yaml:"storage-class"`
	AccessTier       string `yaml:"access-tier"`
	RetryMaxAttempts int    `yaml:"retry-max-attempts"`
	RetryMaxBackoff  int    `yaml:"retry-max-backoff"`
	RetryBackoff     int    `yaml:"retry-backoff"`
	ChunkSize        int    `yaml:"chunk-size"`
}

func (a *AwsS3) ToModelAwsS3() *models.AwsS3 {
	return &models.AwsS3{
		BucketName:             a.BucketName,
		Region:                 a.Region,
		Profile:                a.Profile,
		Endpoint:               a.EndpointOverride,
		AccessKeyID:            a.AccessKeyID,
		SecretAccessKey:        a.SecretAccessKey,
		StorageClass:           a.StorageClass,
		AccessTier:             a.AccessTier,
		RetryMaxAttempts:       a.RetryMaxAttempts,
		RetryMaxBackoffSeconds: a.RetryMaxBackoff,
		RetryBackoffSeconds:    a.RetryBackoff,
		ChunkSize:              a.ChunkSize,
	}
}

type GcpStorage struct {
	KeyFile                string  `yaml:"key-file"`
	BucketName             string  `yaml:"bucket-name"`
	EndpointOverride       string  `yaml:"endpoint-override"`
	RetryMaxAttempts       int     `yaml:"retry-max-attempts"`
	RetryMaxBackoff        int     `yaml:"retry-max-backoff"`
	RetryInitBackoff       int     `yaml:"retry-init-backoff"`
	RetryBackoffMultiplier float64 `yaml:"retry-backoff-multiplier"`
	ChunkSize              int     `yaml:"chunk-size"`
}

func (g *GcpStorage) ToModelGcpStorage() *models.GcpStorage {
	return &models.GcpStorage{
		KeyFile:                 g.KeyFile,
		BucketName:              g.BucketName,
		Endpoint:                g.EndpointOverride,
		RetryMaxAttempts:        g.RetryMaxAttempts,
		RetryBackoffMaxSeconds:  g.RetryMaxBackoff,
		RetryBackoffInitSeconds: g.RetryInitBackoff,
		RetryBackoffMultiplier:  g.RetryBackoffMultiplier,
		ChunkSize:               g.ChunkSize,
	}
}

type AzureBlob struct {
	AccountName      string `yaml:"account-name"`
	AccountKey       string `yaml:"account-key"`
	TenantID         string `yaml:"tenant-id"`
	ClientID         string `yaml:"client-id"`
	ClientSecret     string `yaml:"client-secret"`
	EndpointOverride string `yaml:"endpoint-override"`
	ContainerName    string `yaml:"container-name"`
	AccessTier       string `yaml:"access-tier"`
	RetryMaxAttempts int    `yaml:"retry-max-attempts"`
	RetryTimeout     int    `yaml:"retry-timeout"`
	RetryDelay       int    `yaml:"retry-delay"`
	RetryMaxDelay    int    `yaml:"retry-max-delay"`
}

func (a *AzureBlob) ToModelAzureBlob() *models.AzureBlob {
	return &models.AzureBlob{
		AccountName:          a.AccountName,
		AccountKey:           a.AccountKey,
		TenantID:             a.TenantID,
		ClientID:             a.ClientID,
		ClientSecret:         a.ClientSecret,
		Endpoint:             a.EndpointOverride,
		ContainerName:        a.ContainerName,
		AccessTier:           a.AccessTier,
		RetryMaxAttempts:     a.RetryMaxAttempts,
		RetryTimeoutSeconds:  a.RetryTimeout,
		RetryDelaySeconds:    a.RetryDelay,
		RetryMaxDelaySeconds: a.RetryMaxDelay,
	}
}
