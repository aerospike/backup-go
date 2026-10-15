// Copyright 2024-2026 Aerospike, Inc.
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
	"errors"
	"fmt"
	"time"

	"github.com/aerospike/tools-common-go/secretagent"
)

var secretAgentErrClasses = []struct {
	from, to error
}{
	{from: secretagent.ErrInvalidConfig, to: ErrInvalidConfig},
	{from: secretagent.ErrUnsupported, to: ErrUnsupported},
	{from: secretagent.ErrRequestFailed, to: ErrSecretAgent},
	{from: secretagent.ErrInvalidResponse, to: ErrCorruptData},
}

// classifySecretAgentError attaches the error class matching the
// tools-common-go secretagent sentinel that err wraps.
func classifySecretAgentError(err error) error {
	for _, c := range secretAgentErrClasses {
		if errors.Is(err, c.from) {
			return fmt.Errorf("%w: %w", c.to, err)
		}
	}

	return fmt.Errorf("%w: %w", ErrSecretAgent, err)
}

// getSecret gets the secret from the secret agent using the given client.
func getSecret(ctx context.Context, client *secretagent.Client, key string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("%w: secret config not initialized", ErrInvalidConfig)
	}

	ref, err := secretagent.ParseRef(key)
	if err != nil {
		return "", classifySecretAgentError(err)
	}

	result, err := client.GetSecret(ctx, ref.Resource, ref.Key)
	if err != nil {
		return "", fmt.Errorf("failed to get secret from secret agent: %w", classifySecretAgentError(err))
	}

	return result, nil
}

// ParseSecret checks if the provided string contains a secret key and attempts
// to retrieve the actual secret value from a secret agent, if configured.
//
// If the input string does not contain a secret key it is returned as is,
// without any modification.
func ParseSecret(ctx context.Context, config *SecretAgentConfig, secret string) (string, error) {
	if !secretagent.IsSecret(secret) {
		return secret, nil
	}

	client, err := NewSecretAgentClient(config)
	if err != nil {
		return "", err
	}

	return getSecret(ctx, client, secret)
}

// NewSecretAgentClient initializes a new secret agent client from config.
func NewSecretAgentClient(config *SecretAgentConfig) (*secretagent.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: secret config not initialized", ErrInvalidConfig)
	}

	tlsConfig, err := secretagent.NewTLSConfig(secretagent.TLSOptions{
		CAFile:     deref(config.CaFile),
		CertFile:   deref(config.CertFile),
		KeyFile:    deref(config.KeyFile),
		ServerName: deref(config.TLSName),
		MinVersion: deref(config.MinTLSVersion),
	})
	if err != nil {
		return nil, classifySecretAgentError(err)
	}

	address := deref(config.Address)
	if config.Port != nil {
		address = fmt.Sprintf("%s:%d", address, *config.Port)
	}

	timeout := secretagent.DefaultTimeout
	if config.TimeoutMillisecond != nil {
		timeout = time.Duration(*config.TimeoutMillisecond) * time.Millisecond
	}

	client, err := secretagent.NewClient(secretagent.Config{
		TLS:            tlsConfig,
		ConnectionType: deref(config.ConnectionType),
		Address:        address,
		Timeout:        timeout,
		Base64:         deref(config.IsBase64),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize secret agent client: %w", classifySecretAgentError(err))
	}

	return client, nil
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}

	return *p
}
