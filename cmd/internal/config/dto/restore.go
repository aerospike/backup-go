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

// Restore is used to map yaml config.
type Restore struct {
	App     App     `yaml:"app"`
	Cluster Cluster `yaml:"cluster"`
	Restore struct {
		Directory          string   `yaml:"directory"`
		Namespace          string   `yaml:"namespace"`
		SetList            []string `yaml:"set-list"`
		BinList            []string `yaml:"bin-list"`
		Parallel           int      `yaml:"parallel"`
		NoRecords          bool     `yaml:"no-records"`
		NoIndexes          bool     `yaml:"no-indexes"`
		NoUDFs             bool     `yaml:"no-udfs"`
		RecordsPerSecond   int      `yaml:"records-per-second"`
		MaxRetries         int      `yaml:"max-retries"`
		TotalTimeout       int64    `yaml:"total-timeout"`
		SocketTimeout      int64    `yaml:"socket-timeout"`
		Bandwidth          int64    `yaml:"bandwidth"`
		InputFile          string   `yaml:"input-file"`
		DirectoryList      []string `yaml:"directory-list"`
		ParentDirectory    string   `yaml:"parent-directory"`
		DisableBatchWrites bool     `yaml:"disable-batch-writes"`
		BatchSize          int      `yaml:"batch-size"`
		MaxAsyncBatches    int      `yaml:"max-async-batches"`
		WarmUp             int      `yaml:"warm-up"`
		ExtraTTL           int64    `yaml:"extra-ttl"`
		IgnoreRecordError  bool     `yaml:"ignore-record-error"`
		Uniq               bool     `yaml:"unique"`
		Replace            bool     `yaml:"replace"`
		NoGeneration       bool     `yaml:"no-generation"`
		TimeOut            int64    `yaml:"timeout"`
		RetryBaseTimeout   int64    `yaml:"retry-base-timeout"`
		RetryMultiplier    float64  `yaml:"retry-multiplier"`
		RetryMaxRetries    uint     `yaml:"retry-max-retries"`
		Mode               string   `yaml:"mode"`
		ValidateOnly       bool     `yaml:"validate-only"`
	} `yaml:"restore"`
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

func (r *Restore) ToModelRestore() *models.Restore {
	return &models.Restore{
		Common: models.Common{
			Directory:        r.Restore.Directory,
			Namespace:        r.Restore.Namespace,
			SetList:          strings.Join(r.Restore.SetList, ","),
			BinList:          strings.Join(r.Restore.BinList, ","),
			Parallel:         r.Restore.Parallel,
			NoRecords:        r.Restore.NoRecords,
			NoIndexes:        r.Restore.NoIndexes,
			NoUDFs:           r.Restore.NoUDFs,
			RecordsPerSecond: r.Restore.RecordsPerSecond,
			MaxRetries:       r.Restore.MaxRetries,
			TotalTimeout:     r.Restore.TotalTimeout,
			SocketTimeout:    r.Restore.SocketTimeout,
			Bandwidth:        r.Restore.Bandwidth,
		},
		InputFile:          r.Restore.InputFile,
		DirectoryList:      strings.Join(r.Restore.DirectoryList, ","),
		ParentDirectory:    r.Restore.ParentDirectory,
		DisableBatchWrites: r.Restore.DisableBatchWrites,
		BatchSize:          r.Restore.BatchSize,
		MaxAsyncBatches:    r.Restore.MaxAsyncBatches,
		WarmUp:             r.Restore.WarmUp,
		ExtraTTL:           r.Restore.ExtraTTL,
		IgnoreRecordError:  r.Restore.IgnoreRecordError,
		Uniq:               r.Restore.Uniq,
		Replace:            r.Restore.Replace,
		NoGeneration:       r.Restore.NoGeneration,
		TimeOut:            r.Restore.TimeOut,
		RetryBaseTimeout:   r.Restore.RetryBaseTimeout,
		RetryMultiplier:    r.Restore.RetryMultiplier,
		RetryMaxRetries:    r.Restore.RetryMaxRetries,
		Mode:               r.Restore.Mode,
		ValidateOnly:       r.Restore.ValidateOnly,
	}
}
