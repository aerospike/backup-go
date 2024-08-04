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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	secretAgent "github.com/aerospike/backup-go/pkg/secret-agent"
)

const secretPrefix = "secret:"

// SecretAgentConfig contains secret agent connection information.
// @Description SecretAgentConfig contains secret agent connection information.
type SecretAgentConfig struct {
	// Connection type: tcp, unix.
	// Use constants form `secret-agent`: `ConnectionTypeTCP` or `ConnectionTypeUDS`
	ConnectionType *string `yaml:"sa-connection-type,omitempty" json:"sa-connection-type,omitempty"`
	// Secret agent host for TCP connection or socket file path for UDS connection.
	Address *string `yaml:"sa-address,omitempty" json:"sa-address,omitempty"`
	// Secret agent port (only for TCP connection).
	Port *int `yaml:"sa-port,omitempty" json:"sa-port,omitempty"`
	// Secret agent connection and reading timeout.
	// Default: 1000 millisecond.
	TimeoutMillisecond *int `yaml:"sa-timeout-millisecond,omitempty" json:"sa-timeout-millisecond,omitempty"`
	// Path to ca file for encrypted connection.
	CaFile *string `yaml:"sa-ca-file,omitempty" json:"sa-ca-file,omitempty"`
	// Flag that shows if secret agent responses are encrypted with base64.
	IsBase64 *bool `yaml:"sa-is-base64,omitempty" json:"sa-is-base64,omitempty"`
}

func (s *SecretAgentConfig) Validate() error {
	if s == nil {
		return nil
	}

	// As secret agent is not mandatory, we will validate params only if secret agent is enabled.
	// If ConnectionType is set, we consider that secret agent is enabled.
	if s.ConnectionType != nil {
		if s.Address == nil {
			return fmt.Errorf("secret agent address is required")
		}
	}

	return nil
}

func (s *SecretAgentConfig) GetSecret(key string) (string, error) {
	// Getting resource and key.
	resource, secretKey, err := getResourceKey(key)
	if err != nil {
		return "", err
	}
	// Getting tls config.
	tlsConfig, err := getTlSConfig(s.CaFile)
	if err != nil {
		return "", err
	}

	// Parsing config values.
	address := *s.Address
	if s.Port != nil {
		address = fmt.Sprintf("%s:%d", *s.Address, *s.Port)
	}

	// Parsing timeout, if it is nil, by default we set 1000 Millisecond
	timeout := 1000 * time.Millisecond
	if s.TimeoutMillisecond != nil {
		timeout = time.Duration(*s.TimeoutMillisecond) * time.Millisecond
	}

	// Parsing isBase64 param.
	var isBase64 bool
	if s.IsBase64 != nil {
		isBase64 = *s.IsBase64
	}

	// Initializing client.
	saClient, err := secretAgent.NewClient(
		*s.ConnectionType,
		address,
		timeout,
		isBase64,
		tlsConfig,
	)
	if err != nil {
		return "", fmt.Errorf("failed to initialize secret agent client: %w", err)
	}

	result, err := saClient.GetSecret(resource, secretKey)
	if err != nil {
		return "", fmt.Errorf("failed to get secret from secret agent: %w", err)
	}

	return result, nil
}

func getResourceKey(key string) (resource, secretKey string, err error) {
	keyArr := strings.Split(key, ":")
	if len(keyArr) != 3 {
		return "", "", fmt.Errorf("invalid secret format")
	}
	// We believe that keyArr[0] == secretPrefix
	return keyArr[1], keyArr[2], nil
}

// getTlSConfig returns *tls.Config if caFile is set, or nil if caFile is not set.
func getTlSConfig(caFile *string) (*tls.Config, error) {
	if caFile == nil {
		return nil, nil
	}

	caCert, err := os.ReadFile(*caFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read ca file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	//nolint:gosec // we must support any tls configuration for legacy.
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	return tlsConfig, nil
}

// IsSecret checks if string is secret. e.g.: secrets:resource2:cacert
func IsSecret(secret string) bool {
	return strings.HasPrefix(secret, secretPrefix)
}
