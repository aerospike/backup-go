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

package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	s3DefaultChunkSize = 5 * 1024 * 1024                // 5MB, minimum size of a part
	s3maxFile          = s3DefaultChunkSize * 1_000_000 // 5 TB
	s3type             = "s3"
)

func newS3Client(ctx context.Context, s3Config *Config) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(s3Config.Profile),
		config.WithRegion(s3Config.Region),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to load SDK s3Config, %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if s3Config.Endpoint != "" {
			o.BaseEndpoint = &s3Config.Endpoint
		}

		o.UsePathStyle = true
	})

	return client, nil
}
