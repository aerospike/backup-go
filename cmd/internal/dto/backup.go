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
	"github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/models"
)

// Backup is used to map yaml config.
type Backup struct {
	App struct {
		Verbose  bool   `yaml:"verbose"`
		LogLevel string `yaml:"log-level"`
		LogJSON  bool   `yaml:"log-json"`
	} `yaml:"app"`
	Cluster struct {
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
	} `yaml:"cluster"`
	Backup struct {
		Directory                     string  `yaml:"directory"`
		Namespace                     string  `yaml:"namespace"`
		SetList                       string  `yaml:"set-list"`
		BinList                       string  `yaml:"bin-list"`
		Parallel                      int     `yaml:"parallel"`
		NoRecords                     bool    `yaml:"no-records"`
		NoIndexes                     bool    `yaml:"no-indexes"`
		NoUDFs                        bool    `yaml:"no-udfs"`
		RecordsPerSecond              int     `yaml:"records-per-second"`
		MaxRetries                    int     `yaml:"max-retries"`
		TotalTimeout                  int64   `yaml:"total-timeout"`
		SocketTimeout                 int64   `yaml:"socket-timeout"`
		Nice                          int     `yaml:"nice"`
		OutputFile                    string  `yaml:"remove-files"`
		RemoveFiles                   bool    `yaml:"remove-artifacts"`
		ModifiedBefore                string  `yaml:"output-file"`
		ModifiedAfter                 string  `yaml:"output-file-prefix"`
		FileLimit                     uint64  `yaml:"file-limit"`
		AfterDigest                   string  `yaml:"no-bins"`
		MaxRecords                    int64   `yaml:"no-ttl-only"`
		NoBins                        bool    `yaml:"after-digest"`
		SleepBetweenRetries           int     `yaml:"modified-after"`
		FilterExpression              string  `yaml:"modified-before"`
		ParallelNodes                 bool    `yaml:"filter-exp"`
		RemoveArtifacts               bool    `yaml:"parallel-nodes"`
		Compact                       bool    `yaml:"node-list"`
		NodeList                      string  `yaml:"partition-list"`
		NoTTLOnly                     bool    `yaml:"prefer-racks"`
		PreferRacks                   string  `yaml:"rack-list"`
		PartitionList                 string  `yaml:"max-records"`
		Estimate                      bool    `yaml:"sleep-between-retries"`
		EstimateSamples               int64   `yaml:"compact"`
		StateFileDst                  string  `yaml:"estimate"`
		Continue                      string  `yaml:"estimate-samples"`
		ScanPageSize                  int64   `yaml:"state-file-dst"`
		OutputFilePrefix              string  `yaml:"continue"`
		RackList                      string  `yaml:"scan-page-size"`
		InfoMaxRetries                uint    `yaml:"info-max-retries"`
		InfoRetriesMultiplier         float64 `yaml:"info-retries-multiplier"`
		InfoRetryIntervalMilliseconds int64   `yaml:"info-retry-timeout"`
	} `yaml:"backup"`
	Compression struct {
		Mode  string `yaml:"mode"`
		Level int    `yaml:"level"`
	} `yaml:"compression"`
	Encryption struct {
		Mode      string `yaml:"mode"`
		KeyFile   string `yaml:"key-file"`
		KeyEnv    string `yaml:"key-env"`
		KeySecret string `yaml:"key-secret"`
	} `yaml:"encryption"`
	SecretAgent struct {
		ConnectionType     string `yaml:"connection-type"`
		Address            string `yaml:"address"`
		Port               int    `yaml:"port"`
		TimeoutMillisecond int    `yaml:"timeout-millisecond"`
		CaFile             string `yaml:"ca-file"`
		IsBase64           bool   `yaml:"is-base64"`
	} `yaml:"secret-agent"`
	Aws struct {
		S3 struct {
			BucketName       string `yaml:"bucket-name"`
			Region           string `yaml:"region"`
			Profile          string `yaml:"profile"`
			EndpointOverride string `yaml:"endpoint-override"`
			AccessKeyID      string `yaml:"access-key-id"`
			SecretAccessKey  string `yaml:"secret-access-key"`
			StorageClass     string `yaml:"storage-class"`
			RetryMaxAttempts int    `yaml:"retry-max-attempts"`
			RetryMaxBackoff  int    `yaml:"retry-max-backoff"`
			RetryBackoff     int    `yaml:"retry-backoff"`
			ChunkSize        int    `yaml:"chunk-size"`
		} `yaml:"s3"`
	} `yaml:"aws"`
	Gcp struct {
		Storage struct {
			KeyFile                string  `yaml:"key-file"`
			BucketName             string  `yaml:"bucket-name"`
			EndpointOverride       string  `yaml:"endpoint-override"`
			RetryMaxAttempts       int     `yaml:"retry-max-attempts"`
			RetryMaxBackoff        int     `yaml:"retry-max-backoff"`
			RetryInitBackoff       int     `yaml:"retry-init-backoff"`
			RetryBackoffMultiplier float64 `yaml:"retry-backoff-multiplier"`
			ChunkSize              int     `yaml:"chunk-size"`
		} `yaml:"storage"`
	} `yaml:"gcp"`
	Azure struct {
		Blob struct {
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
		} `yaml:"blob"`
	} `yaml:"azure"`
}

func (b *Backup) ToParams() *config.BackupParams {
	return &config.BackupParams{
		App: &models.App{
			Verbose:  b.App.Verbose,
			LogLevel: b.App.LogLevel,
			LogJSON:  b.App.LogJSON,
		},

		Backup: &models.Backup{
			Common: models.Common{
				Directory:        b.Backup.Directory,
				Namespace:        b.Backup.Namespace,
				SetList:          b.Backup.SetList,
				BinList:          b.Backup.BinList,
				Parallel:         b.Backup.Parallel,
				NoRecords:        b.Backup.NoRecords,
				NoIndexes:        b.Backup.NoIndexes,
				NoUDFs:           b.Backup.NoUDFs,
				RecordsPerSecond: b.Backup.RecordsPerSecond,
				MaxRetries:       b.Backup.MaxRetries,
				TotalTimeout:     b.Backup.TotalTimeout,
				SocketTimeout:    b.Backup.SocketTimeout,
				Nice:             b.Backup.Nice,
			},
			OutputFile:                    b.Backup.OutputFile,
			RemoveFiles:                   b.Backup.RemoveFiles,
			ModifiedBefore:                b.Backup.ModifiedBefore,
			ModifiedAfter:                 b.Backup.ModifiedAfter,
			FileLimit:                     b.Backup.FileLimit,
			AfterDigest:                   b.Backup.AfterDigest,
			MaxRecords:                    b.Backup.MaxRecords,
			NoBins:                        b.Backup.NoBins,
			SleepBetweenRetries:           b.Backup.SleepBetweenRetries,
			FilterExpression:              b.Backup.FilterExpression,
			ParallelNodes:                 b.Backup.ParallelNodes,
			RemoveArtifacts:               b.Backup.RemoveArtifacts,
			Compact:                       b.Backup.Compact,
			NodeList:                      b.Backup.NodeList,
			NoTTLOnly:                     b.Backup.NoTTLOnly,
			PreferRacks:                   b.Backup.PreferRacks,
			PartitionList:                 b.Backup.PartitionList,
			Estimate:                      b.Backup.Estimate,
			EstimateSamples:               b.Backup.EstimateSamples,
			StateFileDst:                  b.Backup.StateFileDst,
			Continue:                      b.Backup.Continue,
			ScanPageSize:                  b.Backup.ScanPageSize,
			OutputFilePrefix:              b.Backup.OutputFilePrefix,
			RackList:                      b.Backup.RackList,
			InfoMaxRetries:                b.Backup.InfoMaxRetries,
			InfoRetriesMultiplier:         b.Backup.InfoRetriesMultiplier,
			InfoRetryIntervalMilliseconds: b.Backup.InfoRetryIntervalMilliseconds,
		},
		Compression: &models.Compression{
			Mode:  b.Compression.Mode,
			Level: b.Compression.Level,
		},
		Encryption: &models.Encryption{
			Mode:      b.Encryption.Mode,
			KeyFile:   b.Encryption.KeyFile,
			KeyEnv:    b.Encryption.KeyEnv,
			KeySecret: b.Encryption.KeySecret,
		},
		SecretAgent: &models.SecretAgent{
			ConnectionType:     b.SecretAgent.ConnectionType,
			Address:            b.SecretAgent.Address,
			Port:               b.SecretAgent.Port,
			TimeoutMillisecond: b.SecretAgent.TimeoutMillisecond,
			CaFile:             b.SecretAgent.CaFile,
			IsBase64:           b.SecretAgent.IsBase64,
		},
		AwsS3: &models.AwsS3{
			BucketName:             b.Aws.S3.BucketName,
			Region:                 b.Aws.S3.Region,
			Profile:                b.Aws.S3.Profile,
			Endpoint:               b.Aws.S3.EndpointOverride,
			AccessKeyID:            b.Aws.S3.AccessKeyID,
			SecretAccessKey:        b.Aws.S3.SecretAccessKey,
			StorageClass:           b.Aws.S3.StorageClass,
			RetryMaxAttempts:       b.Aws.S3.RetryMaxAttempts,
			RetryMaxBackoffSeconds: b.Aws.S3.RetryMaxBackoff,
			RetryBackoffSeconds:    b.Aws.S3.RetryBackoff,
			ChunkSize:              b.Aws.S3.ChunkSize,
		},
		GcpStorage: &models.GcpStorage{
			KeyFile:                 b.Gcp.Storage.KeyFile,
			BucketName:              b.Gcp.Storage.BucketName,
			Endpoint:                b.Gcp.Storage.EndpointOverride,
			RetryMaxAttempts:        b.Gcp.Storage.RetryMaxAttempts,
			RetryBackoffMaxSeconds:  b.Gcp.Storage.RetryMaxBackoff,
			RetryBackoffInitSeconds: b.Gcp.Storage.RetryInitBackoff,
			RetryBackoffMultiplier:  b.Gcp.Storage.RetryBackoffMultiplier,
			ChunkSize:               b.Gcp.Storage.ChunkSize,
		},
		AzureBlob: &models.AzureBlob{
			AccountName:          b.Azure.Blob.AccountName,
			AccountKey:           b.Azure.Blob.AccountKey,
			TenantID:             b.Azure.Blob.TenantID,
			ClientID:             b.Azure.Blob.ClientID,
			ClientSecret:         b.Azure.Blob.ClientSecret,
			Endpoint:             b.Azure.Blob.EndpointOverride,
			ContainerName:        b.Azure.Blob.ContainerName,
			AccessTier:           b.Azure.Blob.AccessTier,
			RetryMaxAttempts:     b.Azure.Blob.RetryMaxAttempts,
			RetryTimeoutSeconds:  b.Azure.Blob.RetryTimeout,
			RetryDelaySeconds:    b.Azure.Blob.RetryDelay,
			RetryMaxDelaySeconds: b.Azure.Blob.RetryMaxDelay,
		},
	}
}
