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
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testEncSAAddress      = ":9090"
	testPKeyFile          = "tests/integration/pkey_test"
	testPKeyEnv           = "testPKeyEnv"
	testPKeyEnvErrContent = "testPKeyEnvErr"
)

func TestIOEncryption_readPemFromFile(t *testing.T) {
	testCases := []struct {
		file       string
		errContent string
	}{
		{testPKeyFile, ""},
		{"", "unable to read PEM file"},
	}

	for i, tt := range testCases {
		_, err := readPemFromFile(tt.file)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
	}
}

func TestIOEncryption_readPemFromEnv(t *testing.T) {
	pemData, err := os.ReadFile(testPKeyFile)
	require.NoError(t, err)
	t.Setenv(testPKeyEnv, string(pemData))

	testCases := []struct {
		keyEnv     string
		errContent string
	}{
		{testPKeyEnv, ""},
		{"", "environment variable"},
	}

	for i, tt := range testCases {
		_, err = readPemFromEnv(tt.keyEnv)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
	}
}

func TestIOEncryption_readPrivateKey(t *testing.T) {
	pemData, err := os.ReadFile(testPKeyFile)
	require.NoError(t, err)
	t.Setenv(testPKeyEnv, string(pemData))
	t.Setenv(testPKeyEnvErrContent, testPKeyEnvErrContent)

	listener, err := mockTCPServer(testEncSAAddress, mockHandler)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	// Wait for server start.
	time.Sleep(1 * time.Second)

	saAddr := testEncSAAddress
	saCfg := testSecretAgentConfig()
	saCfg.Port = nil
	saCfg.Address = &saAddr

	keyFile := testPKeyFile
	encPolicyKeyFile := &EncryptionPolicy{
		KeyFile: &keyFile,
		Mode:    EncryptAES128,
	}
	keyFileErr := ""
	encPolicyKeyFileErr := &EncryptionPolicy{
		KeyFile: &keyFileErr,
		Mode:    EncryptAES128,
	}

	keyEnv := testPKeyEnv
	encPolicyKeyEnv := &EncryptionPolicy{
		KeyEnv: &keyEnv,
		Mode:   EncryptAES128,
	}
	keyEnvErr := ""
	encPolicyKeyEnvErr := &EncryptionPolicy{
		KeyEnv: &keyEnvErr,
		Mode:   EncryptAES128,
	}
	keyEnvErrContent := testPKeyEnvErrContent
	encPolicyKeyEnvErrContent := &EncryptionPolicy{
		KeyEnv: &keyEnvErrContent,
		Mode:   EncryptAES128,
	}

	keySecret := testSASecretKey
	encPolicyKeySecret := &EncryptionPolicy{
		KeySecret: &keySecret,
		Mode:      EncryptAES128,
	}
	keySecretErr := ""
	encPolicyKeySecretErr := &EncryptionPolicy{
		KeySecret: &keySecretErr,
		Mode:      EncryptAES128,
	}

	testCases := []struct {
		encPolicy  *EncryptionPolicy
		saConfig   *SecretAgentConfig
		errContent string
	}{
		{encPolicyKeyFile, saCfg,
			""},
		{encPolicyKeyFileErr, saCfg,
			"unable to read PEM from file"},
		{encPolicyKeyEnv, saCfg,
			""},
		{encPolicyKeyEnvErrContent, saCfg,
			"failed to decode PEM block containing private key"},
		{encPolicyKeyEnvErr, saCfg,
			"unable to read PEM from ENV"},
		{encPolicyKeySecret, saCfg,
			""},
		{encPolicyKeySecretErr, saCfg,
			"unable to read PEM from secret agent"},
	}

	for i, tt := range testCases {
		_, err = readPrivateKey(tt.encPolicy, tt.saConfig)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
	}
}

func Test_parsePK(t *testing.T) {
	t.Parallel()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "failed to generate test RSA key")

	pkcs1Bytes := x509.MarshalPKCS1PrivateKey(rsaKey)

	pkcs8RSABytes, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	require.NoError(t, err, "failed to marshal RSA key to PKCS8")

	tests := []struct {
		name     string
		block    []byte
		wantErr  bool
		errCheck func(err error) bool
	}{
		{
			name:    "valid PKCS8 RSA key",
			block:   pkcs8RSABytes,
			wantErr: false,
		},
		{
			name:    "valid PKCS1 RSA key",
			block:   pkcs1Bytes,
			wantErr: false,
		},
		{
			name:    "invalid data",
			block:   []byte("not a valid key"),
			wantErr: true,
			errCheck: func(err error) bool {
				return err != nil
			},
		},
		{
			name:    "nil input",
			block:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parsePK(tt.block)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					assert.True(t, tt.errCheck(err), "error doesn't match expected condition")
				}
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
				// Verify it's a valid RSA key by checking basic properties
				assert.Positive(t, got.N.BitLen())
				assert.NotNil(t, got.D)
			}
		})
	}
}

func Test_parsePK_WithPEM(t *testing.T) {
	t.Parallel()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pkcs1PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	require.NoError(t, err)
	pkcs8PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	tests := []struct {
		name    string
		pemData []byte
		wantErr bool
	}{
		{
			name:    "PKCS1 PEM",
			pemData: pkcs1PEM,
			wantErr: false,
		},
		{
			name:    "PKCS8 PEM",
			pemData: pkcs8PEM,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			block, _ := pem.Decode(tt.pemData)
			require.NotNil(t, block, "failed to decode PEM")

			got, err := parsePK(block.Bytes)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

func TestProcessKeyContent(t *testing.T) {
	dummyDER := []byte{ // Random binary
		0x30, 0x82, 0x01, 0x22, 0x02, 0x01, 0x00, 0x02, 0x81, 0x81,
		0x00, 0xAF, 0xCD, 0x12, 0x99, 0x22, 0x33, 0x44, 0xFF, 0xEE,
	}

	// The expected Standard PEM Output
	expectedBody := base64.StdEncoding.EncodeToString(dummyDER)
	expectedPEM := []byte("-----BEGIN PRIVATE KEY-----\n" + expectedBody + "\n-----END PRIVATE KEY-----\n")

	// Prepare Input Variants
	// Variant 1: Raw PEM
	rawPEM := string(expectedPEM)
	// Variant 2: Base64 encoded PEM
	b64PEM := base64.StdEncoding.EncodeToString(expectedPEM)

	// Variant 3: Raw Body (Base64 of DER)
	rawBody := expectedBody

	// Variant 4: Double Base64 (Base64 of Raw Body)
	doubleB64 := base64.StdEncoding.EncodeToString([]byte(rawBody))

	// Variant 5: Raw Body with Newlines (Simulating email copy-paste)
	messyBody := rawBody[:10] + "\n" + rawBody[10:20] + "\r\n" + rawBody[20:]

	// Variant 6: One-line PEM (BEGIN + base64 + END all on one line, no spaces in base64)
	oneLinePEM := "-----BEGIN PRIVATE KEY-----" + expectedBody + "-----END PRIVATE KEY-----"

	// Variant 7: PEM newlines replaced with spaces (common UI/Secrets behavior)
	spacesInsteadOfNewlinesPEM := strings.ReplaceAll(string(expectedPEM), "\n", " ")

	tests := []struct {
		name      string
		input     string
		wantBytes []byte
		wantErr   bool
	}{
		{
			name:      "Raw PEM",
			input:     rawPEM,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
		{
			name:      "Base64 Encoded PEM",
			input:     b64PEM,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
		{
			name:      "Raw Body (Base64 DER)",
			input:     rawBody,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
		{
			name:      "Scenario 4: Double Base64",
			input:     doubleB64,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
		{
			name:      "Scenario 5: Messy Whitespace in Base64",
			input:     messyBody,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
		{
			name:      "Scenario 6: Preamble support",
			input:     "Some comments here...\n" + rawPEM,
			wantErr:   false,
			wantBytes: []byte("Some comments here...\n" + rawPEM), // It preserves preamble for Raw PEM
		},
		{
			name:    "Failure: Garbage String",
			input:   "This is not a key",
			wantErr: true,
		},
		{
			name:    "Failure: Invalid Base64 Characters",
			input:   "Invalid@Key!Characters",
			wantErr: true,
		},
		{
			name:    "Failure: Empty String",
			input:   "",
			wantErr: true,
		},
		{
			name:    "Failure: Whitespace only",
			input:   "   \n  \t ",
			wantErr: true,
		},
		{
			name:      "One-line PEM",
			input:     oneLinePEM,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
		{
			name:      "PEM newlines replaced by spaces",
			input:     spacesInsteadOfNewlinesPEM,
			wantErr:   false,
			wantBytes: expectedPEM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeKeyContent(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("decodeKeyContent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(tt.wantBytes)) {
				t.Errorf("decodeKeyContent() got = \n%s\n, want \n%s", got, tt.wantBytes)
			}
		})
	}
}
