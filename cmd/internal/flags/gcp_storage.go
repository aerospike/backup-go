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

package flags

import (
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/spf13/pflag"
)

type GcpStorage struct {
	operation int
	models.GcpStorage
}

func NewGcpStorage(operation int) *GcpStorage {
	return &GcpStorage{
		operation: operation,
	}
}

func (f *GcpStorage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.KeyFile, "gcp-key-path",
		"",
		"Path to file containing service account JSON key.")
	flagSet.StringVar(&f.BucketName, "gcp-bucket-name",
		"",
		"Name of the Google cloud storage bucket.")
	flagSet.StringVar(&f.Endpoint, "gcp-endpoint-override",
		"",
		"An alternate url endpoint to send GCP API calls to.")

	if f.operation == OperationBackup {
		flagSet.IntVar(&f.ChunkSize, "gcp-chunk-size",
			models.DefaultChunkSize,
			"Chunk size controls the maximum number of bytes of the object that the app will attempt to send to\n"+
				"the server in a single request. Objects smaller than the size will be sent in a single request,\n"+
				"while larger objects will be split over multiple requests.")
	}

	flagSet.IntVar(&f.RetryMaxAttempts, "gcp-retry-max-attempts",
		cloudMaxRetries,
		"Max retries specifies the maximum number of attempts a failed operation will be retried\n"+
			"before producing an error.")
	flagSet.IntVar(&f.RetryBackoffMaxSeconds, "gcp-retry-max-backoff",
		cloudMaxBackoff,
		"Max backoff is the maximum value in seconds of the retry period.")
	flagSet.IntVar(&f.RetryBackoffInitSeconds, "gcp-retry-init-backoff",
		cloudBackoff,
		"Initial backoff is the initial value in seconds of the retry period.")
	flagSet.Float64Var(&f.RetryBackoffMultiplier, "gcp-retry-backoff-multiplier",
		2,
		"Multiplier is the factor by which the retry period increases.\n"+
			"It should be greater than 1.")

	return flagSet
}

func (f *GcpStorage) GetGcpStorage() *models.GcpStorage {
	return &f.GcpStorage
}
