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

package backup

import (
	"fmt"
	"testing"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
	"github.com/stretchr/testify/require"
)

func testSecretAgentConfig() *SecretAgentConfig {
	cType := saClient.ConnectionTypeTCP
	address := "127.0.0.1"
	port := 2222
	timeout := 1000
	isBase64 := false

	return &SecretAgentConfig{
		ConnectionType:     &cType,
		Address:            &address,
		Port:               &port,
		TimeoutMillisecond: &timeout,
		IsBase64:           &isBase64,
	}
}

func TestConfigSecretAgent_validate(t *testing.T) {
	t.Parallel()

	cfg := testSecretAgentConfig()

	cfgNoAddressNil := testSecretAgentConfig()
	cfgNoAddressNil.Address = nil

	cfgNoAddressEmpty := testSecretAgentConfig()
	addr := ""
	cfgNoAddressEmpty.Address = &addr

	cfgNegTimeout := testSecretAgentConfig()
	negTimeout := -10
	cfgNegTimeout.TimeoutMillisecond = &negTimeout

	cfgConTypeNil := testSecretAgentConfig()
	cfgConTypeNil.ConnectionType = nil

	cfgConTypeErr := testSecretAgentConfig()
	cTypeErr := "err"
	cfgConTypeErr.ConnectionType = &cTypeErr

	testCases := []struct {
		config     *SecretAgentConfig
		errContent string
	}{
		{
			nil, "",
		},
		{
			cfg, "",
		},
		{
			cfgNoAddressNil, "address is required",
		},
		{
			cfgNoAddressEmpty, "address is required",
		},
		{
			cfgNegTimeout, "invalid timeout",
		},
		{
			cfgConTypeNil, "connection type is required",
		},
		{
			cfgConTypeErr, "unsupported connection type",
		},
	}

	for i, tt := range testCases {
		err := tt.config.validate()
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}
