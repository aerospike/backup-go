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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
)

const secretPrefix = "secrets:"

// getSecret gets the secret from the secret agent using the given client.
func getSecret(ctx context.Context, client *saClient.Client, key string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("secret config not initialized")
	}

	resource, secretKey, err := getResourceKey(key)
	if err != nil {
		return "", err
	}

	result, err := client.GetSecret(ctx, resource, secretKey)
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

// getTLSConfig returns the TLS configuration for the given CA file if it is set.
func getTLSConfig(config *SecretAgentConfig) (*tls.Config, error) {
	if config.CaFile == nil {
		return nil, nil
	}

	caCert, err := os.ReadFile(*config.CaFile)
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

	if config.TLSName != nil && *config.TLSName != "" {
		tlsConfig.ServerName = *config.TLSName
	}

	// If cert and key files are specified, we load them.
	if config.CertFile != nil && config.KeyFile != nil {
		cert, err := tls.LoadX509KeyPair(*config.CertFile, *config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
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
func ParseSecret(ctx context.Context, config *SecretAgentConfig, secret string) (string, error) {
	// If value doesn't contain the secret, we return it as is.
	if !isSecret(secret) {
		return secret, nil
	}

	client, err := NewSecretAgentClient(config)
	if err != nil {
		return "", err
	}

	return getSecret(ctx, client, secret)
}

// NewSecretAgentClient initializes a new secret agent client from config.
// Pass context to GetSecret when performing requests (for cancellation).
func NewSecretAgentClient(config *SecretAgentConfig) (*saClient.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("secret config not initialized")
	}
	// Getting tls config.
	tlsConfig, err := getTLSConfig(config)
	if err != nil {
		return nil, err
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
		*config.ConnectionType,
		address,
		timeout,
		isBase64,
		tlsConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize secret agent client: %w", err)
	}

	return client, nil
}
