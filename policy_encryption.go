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
	"errors"
	"fmt"
	"os"
)

// Encryption modes
const (
	// EncryptNone no encryption.
	EncryptNone = "NONE"
	// EncryptAES128 encryption using AES128 algorithm.
	EncryptAES128 = "AES128"
	// EncryptAES256 encryption using AES256 algorithm.
	EncryptAES256 = "AES256"

	pemTemplate = "-----BEGIN PRIVATE KEY-----\n%s\n-----END PRIVATE KEY-----"
)

// EncryptionPolicy contains backup encryption information.
type EncryptionPolicy struct {
	// The path to the file containing the encryption key.
	KeyFile *string `yaml:"key-file,omitempty" json:"key-file,omitempty"`
	// The name of the environment variable containing the encryption key.
	KeyEnv *string `yaml:"key-env,omitempty" json:"key-env,omitempty"`
	// The secret keyword in Aerospike Secret Agent containing the encryption key.
	KeySecret *string `yaml:"key-secret,omitempty" json:"key-secret,omitempty"`
	// The encryption mode to be used (NONE, AES128, AES256)
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty" default:"NONE" enums:"NONE,AES128,AES256"`
}

// Validate validates the encryption policy.
func (p *EncryptionPolicy) Validate() error {
	if p == nil {
		return nil
	}

	if p.Mode != EncryptNone && p.Mode != EncryptAES128 && p.Mode != EncryptAES256 {
		return fmt.Errorf("invalid encryption mode: %s", p.Mode)
	}

	if p.KeyFile == nil && p.KeyEnv == nil && p.KeySecret == nil {
		return errors.New("encryption key location not specified")
	}

	// Only one parameter allowed to be set.
	if (p.KeyFile != nil && p.KeyEnv != nil) ||
		(p.KeyFile != nil && p.KeySecret != nil) ||
		(p.KeyEnv != nil && p.KeySecret != nil) {
		return fmt.Errorf("only one encryption key source may be specified")
	}

	return nil
}

// ReadPrivateKey parses and loads a private key according to the EncryptionPolicy
// configuration. It can load the private key from a file, env variable or Secret Agent.
// A valid agent parameter is required to load the key from Aerospike Secret Agent.
// Pass in nil for any other option.
func (p *EncryptionPolicy) ReadPrivateKey(agent *SecretAgentConfig) ([]byte, error) {
	var (
		pemData []byte
		err     error
	)

	switch {
	case p.KeyFile != nil:
		pemData, err = p.readPemFromFile()
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from file: %w", err)
		}
	case p.KeyEnv != nil:
		pemData, err = p.readPemFromEnv()
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from ENV: %w", err)
		}
	case p.KeySecret != nil:
		pemData, err = p.readPemFromSecret(agent)
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

	if p.Mode == EncryptAES128 {
		return sum256[:16], nil
	}

	return sum256[:], nil
}

func (p *EncryptionPolicy) readPemFromFile() ([]byte, error) {
	pemData, err := os.ReadFile(*p.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read PEM file: %w", err)
	}

	return pemData, nil
}

// readPemFromEnv reads the key from an env variable encrypted in base64 without header
// and footer, decrypts it adding the header and footer.
func (p *EncryptionPolicy) readPemFromEnv() ([]byte, error) {
	key := os.Getenv(*p.KeyEnv)
	if key == "" {
		return nil, fmt.Errorf("environment variable %s not set", *p.KeyEnv)
	}

	// add header and footer to make it parsable
	pemKey := fmt.Sprintf(pemTemplate, key)

	return []byte(pemKey), nil
}

func (p *EncryptionPolicy) readPemFromSecret(agent *SecretAgentConfig) ([]byte, error) {
	if agent == nil {
		return nil, fmt.Errorf("secret agent not initialized")
	}

	key, err := agent.GetSecret(*p.KeySecret)
	if err != nil {
		return nil, fmt.Errorf("unable to read secret agent key: %w", err)
	}

	pemKey := fmt.Sprintf(pemTemplate, key)

	return []byte(pemKey), nil
}
