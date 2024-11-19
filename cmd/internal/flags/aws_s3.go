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

type AwsS3 struct {
	models.AwsS3
}

func NewAwsS3() *AwsS3 {
	return &AwsS3{}
}

func (f *AwsS3) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.BucketName, "s3-bucket-name",
		"",
		"Existing S3 bucket name")
	flagSet.StringVar(&f.Region, "s3-region",
		"",
		"The S3 region that the bucket(s) exist in.")
	flagSet.StringVar(&f.Profile, "s3-profile",
		"",
		"The S3 profile to use for credentials.")
	flagSet.StringVar(&f.Endpoint, "s3-endpoint-override",
		"",
		"An alternate url endpoint to send S3 API calls to.")

	return flagSet
}

func (f *AwsS3) GetAwsS3() *models.AwsS3 {
	return &f.AwsS3
}
