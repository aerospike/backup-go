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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAwsS3_NewFlagSet(t *testing.T) {
	t.Parallel()
	awsS3 := NewAwsS3()

	flagSet := awsS3.NewFlagSet()

	args := []string{
		"--s3-region", "us-west-2",
		"--s3-profile", "my-profile",
		"--s3-endpoint-overrid", "https://s3.custom-endpoint.com",
		"--s3-min-part-size", "4000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := awsS3.GetAwsS3()

	assert.Equal(t, "us-west-2", result.Region, "The s3-region flag should be parsed correctly")
	assert.Equal(t, "my-profile", result.Profile, "The s3-profile flag should be parsed correctly")
	assert.Equal(t, "https://s3.custom-endpoint.com", result.Endpoint, "The s3-endpoint-overrid flag should be parsed correctly")
	assert.Equal(t, 4000, result.MinPartSize, "The s3-min-part-size flag should be parsed correctly")
}

func TestAwsS3_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	awsS3 := NewAwsS3()

	flagSet := awsS3.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := awsS3.GetAwsS3()

	assert.Equal(t, "", result.Region, "The default value for s3-region should be an empty string")
	assert.Equal(t, "default", result.Profile, "The default value for s3-profile should be 'default'")
	assert.Equal(t, "", result.Endpoint, "The default value for s3-endpoint-overrid should be an empty string")
	assert.Equal(t, 3005, result.MinPartSize, "The default value for s3-min-part-size should be 3005")
}
