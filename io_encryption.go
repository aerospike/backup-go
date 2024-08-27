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
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

const pemTemplate = "-----BEGIN PRIVATE KEY-----\n%s\n-----END PRIVATE KEY-----"

// ReadPrivateKey parses and loads a private key according to the EncryptionPolicy
// configuration. It can load the private key from a file, env variable or Secret Agent.
// A valid agent parameter is required to load the key from Aerospike Secret Agent.
// Pass in nil for any other option.
func ReadPrivateKey(encPolicy *EncryptionPolicy, saConfig *SecretAgentConfig) ([]byte, error) {
	var (
		pemData []byte
		err     error
	)

	switch {
	case encPolicy.KeyFile != nil:
		pemData, err = readPemFromFile(*encPolicy.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from file: %w", err)
		}
	case encPolicy.KeyEnv != nil:
		pemData, err = readPemFromEnv(*encPolicy.KeyEnv)
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from ENV: %w", err)
		}
	case encPolicy.KeySecret != nil:
		pemData, err = readPemFromSecret(*encPolicy.KeySecret, saConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from secret agent: %w", err)
		}
	}

	// Decode the PEM file
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	key := privateKey.(*rsa.PrivateKey)
	// Originally asbackup converts the key to the PKCS1 format
	decodedKey := x509.MarshalPKCS1PrivateKey(key)

	// AES requires 128 or 256 bits for the key
	sum256 := sha256.Sum256(decodedKey)

	if encPolicy.Mode == EncryptAES128 {
		return sum256[:16], nil
	}

	return sum256[:], nil
}

// readPemFromFile reads the key from the file.
func readPemFromFile(file string) ([]byte, error) {
	pemData, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("unable to read PEM file: %w", err)
	}

	return pemData, nil
}

// readPemFromEnv reads the key from an env variable encrypted in base64 without header
// and footer, decrypts it adding the header and footer.
func readPemFromEnv(keyEnv string) ([]byte, error) {
	key := os.Getenv(keyEnv)
	if key == "" {
		return nil, fmt.Errorf("environment variable %s not set", keyEnv)
	}

	// add header and footer to make it parsable
	pemKey := fmt.Sprintf(pemTemplate, key)

	return []byte(pemKey), nil
}

// readPemFromSecret reads the key from secret agent without a header
// and footer, decrypts it adding the header and footer.
func readPemFromSecret(secret string, config *SecretAgentConfig) ([]byte, error) {
	key, err := getSecret(config, secret)
	if err != nil {
		return nil, fmt.Errorf("unable to read secret config key: %w", err)
	}

	pemKey := fmt.Sprintf(pemTemplate, key)

	return []byte(pemKey), nil
}
