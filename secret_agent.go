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

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
)

const secretPrefix = "secrets:"

// getSecret gets the secret from the secret agent. It returns the secret value or an error if any.
func getSecret(config *SecretAgentConfig, key string) (string, error) {
	if config == nil {
		return "", fmt.Errorf("secret config not initialized")
	}
	// Getting resource and key.
	resource, secretKey, err := getResourceKey(key)
	if err != nil {
		return "", err
	}
	// Getting tls config.
	tlsConfig, err := getTlSConfig(config.CaFile)
	if err != nil {
		return "", err
	}

	// Parsing config values.
	address := *config.Address
	if config.Port != nil {
		address = fmt.Sprintf("%s:%d", *config.Address, *config.Port)
	}

	// Parsing timeout, if it is a nil, by default we set 1000 Millisecond
	timeout := 1000 * time.Millisecond
	if config.TimeoutMillisecond != nil {
		timeout = time.Duration(*config.TimeoutMillisecond) * time.Millisecond
	}

	// Parsing isBase64 param.
	var isBase64 bool
	if config.IsBase64 != nil {
		isBase64 = *config.IsBase64
	}

	// Initializing client.
	client, err := saClient.NewClient(
		saClient.ConnectionType(*config.ConnectionType),
		address,
		timeout,
		isBase64,
		tlsConfig,
	)
	if err != nil {
		return "", fmt.Errorf("failed to initialize secret agent client: %w", err)
	}

	result, err := client.GetSecret(resource, secretKey)
	if err != nil {
		return "", fmt.Errorf("failed to get secret from secret agent: %w", err)
	}

	return result, nil
}

// getResourceKey returns the resource and secret key for the given secret key.
func getResourceKey(key string) (resource, secretKey string, err error) {
	if !isSecret(key) {
		return "", "",
			fmt.Errorf("invalid secret key format, must be secrets:<resource>:<secret>")
	}

	keyArr := strings.Split(key, ":")
	if len(keyArr) != 3 {
		return "", "", fmt.Errorf("invalid secret key format")
	}
	// We believe that keyArr[0] == secretPrefix
	return keyArr[1], keyArr[2], nil
}

// getTlSConfig returns the TLS configuration for the given CA file if it is set.
func getTlSConfig(caFile *string) (*tls.Config, error) {
	if caFile == nil {
		return nil, nil
	}

	caCert, err := os.ReadFile(*caFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read ca file: %w", err)
	}

	caCertPool := x509.NewCertPool()

	ok := caCertPool.AppendCertsFromPEM(caCert)
	if !ok {
		return nil, fmt.Errorf("nothing to append to ca cert pool")
	}

	//nolint:gosec // we must support any tls configuration for legacy.
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	return tlsConfig, nil
}

// isSecret checks if string is secret. e.g.: secrets:resource2:cacert
func isSecret(secret string) bool {
	return strings.HasPrefix(secret, secretPrefix)
}

// ParseSecret checks if the provided string contains a secret key and attempts
// to retrieve the actual secret value from a secret agent, if configured.
//
// If the input string does not contain a secret key it is returned as is,
// without any modification.
func ParseSecret(config *SecretAgentConfig, secret string) (string, error) {
	// If value doesn't contain the secret, we return it as is.
	if !isSecret(secret) {
		return secret, nil
	}

	return getSecret(config, secret)
}
