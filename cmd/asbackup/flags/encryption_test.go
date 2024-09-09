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

func TestEncryption_NewFlagSet(t *testing.T) {
	t.Parallel()
	encryption := NewEncryption()

	flagSet := encryption.NewFlagSet()

	args := []string{
		"--encrypt", "AES256",
		"--encryption-key-file", "/path/to/key.pem",
		"--encryption-key-env", "MY_ENV_KEY",
		"--encryption-key-secret", "my-secret",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := encryption.GetEncryption()

	assert.Equal(t, "AES256", result.Mode, "The encrypt flag should be parsed correctly")
	assert.Equal(t, "/path/to/key.pem", result.KeyFile, "The encryption-key-file flag should be parsed correctly")
	assert.Equal(t, "MY_ENV_KEY", result.KeyEnv, "The encryption-key-env flag should be parsed correctly")
	assert.Equal(t, "my-secret", result.KeySecret, "The encryption-key-secret flag should be parsed correctly")
}

func TestEncryption_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	encryption := NewEncryption()

	flagSet := encryption.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := encryption.GetEncryption()

	assert.Equal(t, "", result.Mode, "The default value for encrypt should be an empty string")
	assert.Equal(t, "", result.KeyFile, "The default value for encryption-key-file should be an empty string")
	assert.Equal(t, "", result.KeyEnv, "The default value for encryption-key-env should be an empty string")
	assert.Equal(t, "", result.KeySecret, "The default value for encryption-key-secret should be an empty string")
}
