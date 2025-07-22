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
	"strings"

	"github.com/aerospike/backup-go/cmd/internal/models"
)

// Backup is used to map yaml config.
type Backup struct {
	App     App     `yaml:"app"`
	Cluster Cluster `yaml:"cluster"`
	Backup  struct {
		Directory                     string   `yaml:"directory"`
		Namespace                     string   `yaml:"namespace"`
		SetList                       []string `yaml:"set-list"`
		BinList                       []string `yaml:"bin-list"`
		Parallel                      int      `yaml:"parallel"`
		NoRecords                     bool     `yaml:"no-records"`
		NoIndexes                     bool     `yaml:"no-indexes"`
		NoUDFs                        bool     `yaml:"no-udfs"`
		RecordsPerSecond              int      `yaml:"records-per-second"`
		MaxRetries                    int      `yaml:"max-retries"`
		TotalTimeout                  int64    `yaml:"total-timeout"`
		SocketTimeout                 int64    `yaml:"socket-timeout"`
		Nice                          int64    `yaml:"nice"`
		OutputFile                    string   `yaml:"output-file"`
		RemoveFiles                   bool     `yaml:"remove-files"`
		ModifiedBefore                string   `yaml:"modified-before"`
		ModifiedAfter                 string   `yaml:"modified-after"`
		FileLimit                     uint64   `yaml:"file-limit"`
		AfterDigest                   string   `yaml:"after-digest"`
		MaxRecords                    int64    `yaml:"max-records"`
		NoBins                        bool     `yaml:"no-bins"`
		SleepBetweenRetries           int      `yaml:"sleep-between-retries"`
		FilterExpression              string   `yaml:"filter-exp"`
		ParallelNodes                 bool     `yaml:"parallel-nodes"`
		RemoveArtifacts               bool     `yaml:"remove-artifacts"`
		Compact                       bool     `yaml:"compact"`
		NodeList                      []string `yaml:"node-list"`
		NoTTLOnly                     bool     `yaml:"no-ttl-only"`
		PreferRacks                   []string `yaml:"prefer-racks"`
		PartitionList                 []string `yaml:"partition-list"`
		Estimate                      bool     `yaml:"estimate"`
		EstimateSamples               int64    `yaml:"estimate-samples"`
		StateFileDst                  string   `yaml:"state-file-dst"`
		Continue                      string   `yaml:"continue"`
		ScanPageSize                  int64    `yaml:"scan-page-size"`
		OutputFilePrefix              string   `yaml:"output-file-prefix"`
		RackList                      []string `yaml:"rack-list"`
		InfoMaxRetries                uint     `yaml:"info-max-retries"`
		InfoRetriesMultiplier         float64  `yaml:"info-retries-multiplier"`
		InfoRetryIntervalMilliseconds int64    `yaml:"info-retry-timeout"`
	} `yaml:"backup"`
	Compression Compression `yaml:"compression"`
	Encryption  Encryption  `yaml:"encryption"`
	SecretAgent SecretAgent `yaml:"secret-agent"`
	Aws         struct {
		S3 AwsS3 `yaml:"s3"`
	} `yaml:"aws"`
	Gcp struct {
		Storage GcpStorage `yaml:"storage"`
	} `yaml:"gcp"`
	Azure struct {
		Blob AzureBlob `yaml:"blob"`
	} `yaml:"azure"`
}

func (b *Backup) ToModelBackup() *models.Backup {
	return &models.Backup{
		Common: models.Common{
			Directory:        b.Backup.Directory,
			Namespace:        b.Backup.Namespace,
			SetList:          strings.Join(b.Backup.SetList, ","),
			BinList:          strings.Join(b.Backup.BinList, ","),
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
		NodeList:                      strings.Join(b.Backup.NodeList, ","),
		NoTTLOnly:                     b.Backup.NoTTLOnly,
		PreferRacks:                   strings.Join(b.Backup.PreferRacks, ","),
		PartitionList:                 strings.Join(b.Backup.PartitionList, ","),
		Estimate:                      b.Backup.Estimate,
		EstimateSamples:               b.Backup.EstimateSamples,
		StateFileDst:                  b.Backup.StateFileDst,
		Continue:                      b.Backup.Continue,
		ScanPageSize:                  b.Backup.ScanPageSize,
		OutputFilePrefix:              b.Backup.OutputFilePrefix,
		RackList:                      strings.Join(b.Backup.RackList, ","),
		InfoMaxRetries:                b.Backup.InfoMaxRetries,
		InfoRetriesMultiplier:         b.Backup.InfoRetriesMultiplier,
		InfoRetryIntervalMilliseconds: b.Backup.InfoRetryIntervalMilliseconds,
	}
}
