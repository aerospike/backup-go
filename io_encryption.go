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
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
)

var (
	beginMarkerRegex = regexp.MustCompile(`(-{5}BEGIN [^-]+-{5})\s*`)
	endMarkerRegex   = regexp.MustCompile(`\s*(-{5}END [^-]+-{5})`)
)

// readPrivateKey parses and loads a private key according to the EncryptionPolicy
// configuration. It can load the private key from a file, env variable or Secret Agent.
// secretAgent must be non-nil when using KeySecret; pass nil for file/env-only loading.
func readPrivateKey(ctx context.Context, encPolicy *EncryptionPolicy, secretAgent *saClient.Client) ([]byte, error) {
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
		pemData, err = readPemFromSecret(ctx, *encPolicy.KeySecret, secretAgent)
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from secret agent: %w", err)
		}
	}

	// Decode the PEM file
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	key, err := parsePK(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Originally asbackup converts the key to the PKCS1 format
	decodedKey := x509.MarshalPKCS1PrivateKey(key)

	// AES requires 128 or 256 bits for the key
	sum256 := sha256.Sum256(decodedKey)

	if encPolicy.Mode == EncryptAES128 {
		return sum256[:16], nil
	}

	return sum256[:], nil
}

// resolveEncryptionKey returns the encryption key for the given policy, creating a
// secret agent client when config is set. Call this once at startup; pass the
// result to the file writer/reader processor. Returns (nil, nil) when encryption
// is disabled.
func resolveEncryptionKey(
	ctx context.Context,
	encryptionPolicy *EncryptionPolicy,
	secretAgentConfig *SecretAgentConfig,
) ([]byte, error) {
	if encryptionPolicy == nil || encryptionPolicy.Mode == EncryptNone {
		return nil, nil
	}

	var secretAgentClient *saClient.Client

	if secretAgentConfig != nil {
		var err error

		secretAgentClient, err = NewSecretAgentClient(secretAgentConfig)
		if err != nil {
			return nil, err
		}
	}

	key, err := readPrivateKey(ctx, encryptionPolicy, secretAgentClient)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve encryption key: %w", err)
	}

	return key, nil
}

// parsePK parses a private key from PKCS8 or PKCS1.
func parsePK(block []byte) (*rsa.PrivateKey, error) {
	// Try a PKCS8 format first.
	privateKey, err8 := x509.ParsePKCS8PrivateKey(block)
	if err8 == nil {
		rsaKey, ok := privateKey.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("expected RSA private key, got %T", privateKey)
		}

		return rsaKey, nil
	}

	// Try a PKCS1 format (which is always RSA).
	pkcs1Key, err1 := x509.ParsePKCS1PrivateKey(block)
	if err1 == nil {
		return pkcs1Key, nil
	}

	return nil, errors.Join(err8, err1)
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

	return decodeKeyContent(key)
}

// readPemFromSecret reads the key from secret agent without a header
// and footer, decrypts it adding the header and footer.
func readPemFromSecret(ctx context.Context, secret string, client *saClient.Client) ([]byte, error) {
	key, err := getSecret(ctx, client, secret)
	if err != nil {
		return nil, fmt.Errorf("unable to read secret config key: %w", err)
	}

	return decodeKeyContent(key)
}

// decodeKeyContent handles various formats of the key string.
// It supports:
// 1. Raw PEM (contains -----BEGIN)
// 2. Base64 encoded PEM
// 3. Raw Body (Base64 encoded DER)
// 4. Base64 encoded Raw Body (Double Base64)
func decodeKeyContent(key string) ([]byte, error) {
	if key == "" {
		return nil, errors.New("key is empty")
	}

	keyTrimmed := strings.TrimSpace(key)
	if keyTrimmed == "" {
		return nil, errors.New("key contains only whitespace")
	}

	keyTrimmed = ensurePEMMarkerNewlines(keyTrimmed)

	// --- SCENARIO 1: Raw PEM ---
	// Check if it already has the PEM header.
	// We use Contains because PEM files allow text (preamble) before the header.
	if strings.Contains(keyTrimmed, "-----BEGIN") {
		return []byte(keyTrimmed), nil
	}

	// Attempt First Decode
	decodedBytes, err := base64.StdEncoding.DecodeString(keyTrimmed)
	if err != nil {
		return nil, fmt.Errorf("failed to decode PEM block containing private key: %w", err)
	}

	// --- SCENARIO 2: Base64 encoded PEM ---
	// We decoded it, and found it contains a PEM header inside.
	decodedString := string(decodedBytes)
	if strings.Contains(decodedString, "-----BEGIN") {
		return decodedBytes, nil
	}

	// --- SCENARIO 3 vs 4 (The "Double Base64" Check) ---
	// At this point, 'decodedBytes' is either:
	// A) Binary Key Data (DER). This corresponds to Scenario 3.
	// B) A Text String (MII...) which is *another* Base64 layer. This is Scenario 4.

	// We try to decode one more time.
	innerBytes, err := base64.StdEncoding.DecodeString(decodedString)
	if err == nil {
		// Success! The payload was text and valid Base64.
		// This means the input was "Double Base64".
		return encodeToPEM(innerBytes), nil
	}

	// Failure! The payload was binary DER (or just invalid text).
	// This means the input was standard "Raw Body" (Scenario 3).
	return encodeToPEM(decodedBytes), nil
}

// encodeToPEM wraps binary DER data into a canonical PEM block.
func encodeToPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	})
}

func ensurePEMMarkerNewlines(s string) string {
	s = strings.TrimSpace(s)

	// put a newline after BEGIN ...----- if missing
	s = beginMarkerRegex.ReplaceAllString(s, "$1\n")

	// put a newline before -----END ...----- if missing
	s = endMarkerRegex.ReplaceAllString(s, "\n$1")

	return s
}
