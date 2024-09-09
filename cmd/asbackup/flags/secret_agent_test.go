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

func TestSecretAgent_NewFlagSet(t *testing.T) {
	t.Parallel()
	secretAgent := NewSecretAgent()

	flagSet := secretAgent.NewFlagSet()

	args := []string{
		"--sa-connection-type", "unix",
		"--sa-address", "/tmp/secret-agent.sock",
		"--sa-port", "8080",
		"--sa-timeout", "5000",
		"--sa-cafile", "/path/to/ca.pem",
		"--sa-is-base64",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := secretAgent.GetSecretAgent()

	assert.Equal(t, "unix", result.ConnectionType)
	assert.Equal(t, "/tmp/secret-agent.sock", result.Address)
	assert.Equal(t, 8080, result.Port)
	assert.Equal(t, 5000, result.TimeoutMillisecond)
	assert.Equal(t, "/path/to/ca.pem", result.CaFile)
	assert.True(t, result.IsBase64)
}

func TestSecretAgent_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	secretAgent := NewSecretAgent()

	flagSet := secretAgent.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := secretAgent.GetSecretAgent()

	assert.Equal(t, "tcp", result.ConnectionType)
	assert.Equal(t, "", result.Address)
	assert.Equal(t, 0, result.Port)
	assert.Equal(t, 0, result.TimeoutMillisecond)
	assert.Equal(t, "", result.CaFile)
	assert.False(t, result.IsBase64)
}
