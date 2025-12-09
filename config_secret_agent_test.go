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
	"testing"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
	"github.com/stretchr/testify/require"
)

func testSecretAgentConfig() *SecretAgentConfig {
	cType := saClient.ConnectionTypeTCP
	address := "127.0.0.1"
	port := testSAPort
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

	cfgValid := testSecretAgentConfig()

	cfgValidUDS := testSecretAgentConfig()
	cTypeUDS := saClient.ConnectionTypeUDS
	addrUDS := "/tmp/secret-agent.sock"
	cfgValidUDS.ConnectionType = &cTypeUDS
	cfgValidUDS.Address = &addrUDS

	cfgValidTLS := testSecretAgentConfig()
	caFile := "/path/to/ca.pem"
	tlsName := "server.local"
	certFile := "/path/to/cert.pem"
	keyFile := "/path/to/key.pem"
	cfgValidTLS.CaFile = &caFile
	cfgValidTLS.TLSName = &tlsName
	cfgValidTLS.CertFile = &certFile
	cfgValidTLS.KeyFile = &keyFile

	cfgNoAddressNil := testSecretAgentConfig()
	cfgNoAddressNil.Address = nil

	cfgNoAddressEmpty := testSecretAgentConfig()
	addrEmpty := ""
	cfgNoAddressEmpty.Address = &addrEmpty

	cfgZeroTimeout := testSecretAgentConfig()
	zeroTimeout := 0
	cfgZeroTimeout.TimeoutMillisecond = &zeroTimeout

	cfgNegTimeout := testSecretAgentConfig()
	negTimeout := -10
	cfgNegTimeout.TimeoutMillisecond = &negTimeout

	cfgConTypeNil := testSecretAgentConfig()
	cfgConTypeNil.ConnectionType = nil

	cfgConTypeInvalid := testSecretAgentConfig()
	cTypeInvalid := "invalid"
	cfgConTypeInvalid.ConnectionType = &cTypeInvalid

	cfgCertNoKey := testSecretAgentConfig()
	cert := "/path/to/cert.pem"
	cfgCertNoKey.CertFile = &cert

	cfgKeyNoCert := testSecretAgentConfig()
	key := "/path/to/key.pem"
	cfgKeyNoCert.KeyFile = &key

	testCases := []struct {
		name       string
		config     *SecretAgentConfig
		errContent string
	}{
		{
			name:       "nil config",
			config:     nil,
			errContent: "",
		},
		{
			name:       "valid TCP config",
			config:     cfgValid,
			errContent: "",
		},
		{
			name:       "valid UDS config",
			config:     cfgValidUDS,
			errContent: "",
		},
		{
			name:       "valid config with TLS",
			config:     cfgValidTLS,
			errContent: "",
		},
		{
			name:       "address is nil",
			config:     cfgNoAddressNil,
			errContent: "address is required",
		},
		{
			name:       "address is empty",
			config:     cfgNoAddressEmpty,
			errContent: "address is required",
		},
		{
			name:       "timeout is zero",
			config:     cfgZeroTimeout,
			errContent: "invalid timeout",
		},
		{
			name:       "timeout is negative",
			config:     cfgNegTimeout,
			errContent: "invalid timeout",
		},
		{
			name:       "connection type is nil",
			config:     cfgConTypeNil,
			errContent: "connection type is required",
		},
		{
			name:       "connection type is invalid",
			config:     cfgConTypeInvalid,
			errContent: "unsupported connection type",
		},
		{
			name:       "cert file without key file",
			config:     cfgCertNoKey,
			errContent: "key file is required when cert file is set",
		},
		{
			name:       "key file without cert file",
			config:     cfgKeyNoCert,
			errContent: "cert file is required when key file is set",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.validate()
			if tt.errContent != "" {
				require.ErrorContains(t, err, tt.errContent)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
