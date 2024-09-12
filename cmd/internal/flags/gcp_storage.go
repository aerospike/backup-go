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
	models.GcpStorage
}

func NewGcpStorage() *GcpStorage {
	return &GcpStorage{}
}

func (f *GcpStorage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.KeyFile, "gcp-key-path",
		"",
		"Path to file containing Service Account JSON Key.")
	flagSet.StringVar(&f.BucketName, "gcp-bucket-name",
		"",
		"Name of the Google Cloud Storage bucket.")
	flagSet.StringVar(&f.Endpoint, "gcp-endpoint-override",
		"",
		"An alternate url endpoint to send GCP API calls to.")

	return flagSet
}

func (f *GcpStorage) GetGcpStorage() *models.GcpStorage {
	return &f.GcpStorage
}
