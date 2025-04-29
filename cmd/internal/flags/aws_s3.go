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
	operation int
	models.AwsS3
}

func NewAwsS3(operation int) *AwsS3 {
	return &AwsS3{
		operation: operation,
	}
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
	flagSet.StringVar(&f.AccessKeyID, "s3-access-key-id",
		"",
		"S3 access key id. If not set, profile auth info will be used.")
	flagSet.StringVar(&f.SecretAccessKey, "s3-secret-access-key",
		"",
		"S3 secret access key. If not set, profile auth info will be used.")
	flagSet.StringVar(&f.Endpoint, "s3-endpoint-override",
		"",
		"An alternate url endpoint to send S3 API calls to.")

	switch f.operation {
	case OperationBackup:
		flagSet.StringVar(&f.StorageClass, "s3-storage-class",
			"",
			"Apply storage class to backup files. Storage classes are:\n"+
				"STANDARD,\n"+
				"REDUCED_REDUNDANCY,\n"+
				"STANDARD_IA,\n"+
				"ONEZONE_IA,\n"+
				"INTELLIGENT_TIERING,\n"+
				"GLACIER,\n"+
				"DEEP_ARCHIVE,\n"+
				"OUTPOSTS,\n"+
				"GLACIER_IR,\n"+
				"SNOW,\n"+
				"EXPRESS_ONEZONE.")
		flagSet.IntVar(&f.ChunkSize, "s3-chunk-size",
			5242880,
			"Chunk size controls the maximum number of bytes of the object that the app will attempt to send to\n"+
				"the server in a single request. Objects smaller than the size will be sent in a single request,\n"+
				"while larger objects will be split over multiple requests. ")
	case OperationRestore:
		flagSet.StringVar(&f.AccessTier, "s3-tier",
			"",
			"If is set, tool will try to restore archived files to the specified tier.\n"+
				"Tiers are: Standard, Bulk, Expedited.")
		flagSet.Int64Var(&f.RestorePollDuration, "s3-restore-poll-duration",
			60000,
			"How often (in milliseconds) a backup client checks object status when restoring an archived object.",
		)
	}

	return flagSet
}

func (f *AwsS3) GetAwsS3() *models.AwsS3 {
	return &f.AwsS3
}
