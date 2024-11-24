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

func TestClientPolicy_NewFlagSet(t *testing.T) {
	t.Parallel()
	clientPolicy := NewClientPolicy()

	flagSet := clientPolicy.NewFlagSet()

	args := []string{
		"--client-timeout", "50000",
		"--client-idle-timeout", "2000",
		"--client-login-timeout", "15000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := clientPolicy.GetClientPolicy()

	assert.Equal(t, int64(50000), result.Timeout, "The client-timeout flag should be parsed correctly")
	assert.Equal(t, int64(2000), result.IdleTimeout, "The client-idle-timeout flag should be parsed correctly")
	assert.Equal(t, int64(15000), result.LoginTimeout, "The client-login-timeout flag should be parsed correctly")
}

func TestClientPolicy_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	clientPolicy := NewClientPolicy()

	flagSet := clientPolicy.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := clientPolicy.GetClientPolicy()

	// Verify default values
	assert.Equal(t, int64(30000), result.Timeout, "The default value for client-timeout should be 30000")
	assert.Equal(t, int64(0), result.IdleTimeout, "The default value for client-idle-timeout should be 0")
	assert.Equal(t, int64(10000), result.LoginTimeout, "The default value for client-login-timeout should be 10000")
}
