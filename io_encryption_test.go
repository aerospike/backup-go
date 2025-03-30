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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
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
	t.Parallel()

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
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}

func TestIOEncryption_readPemFromEnv(t *testing.T) {
	t.Parallel()

	pemData, err := os.ReadFile(testPKeyFile)
	require.NoError(t, err)
	err = os.Setenv(testPKeyEnv, string(pemData))
	require.NoError(t, err)

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
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}

func TestIOEncryption_readPrivateKey(t *testing.T) {
	t.Parallel()

	pemData, err := os.ReadFile(testPKeyFile)
	require.NoError(t, err)
	err = os.Setenv(testPKeyEnv, string(pemData))
	require.NoError(t, err)
	err = os.Setenv(testPKeyEnvErrContent, testPKeyEnvErrContent)
	require.NoError(t, err)

	listener, err := mockTCPServer(testEncSAAddress, mockHandler)
	require.NoError(t, err)
	defer listener.Close()

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
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
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
				assert.Error(t, err)
				if tt.errCheck != nil {
					assert.True(t, tt.errCheck(err), "error doesn't match expected condition")
				}
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, got)
				// Verify it's a valid RSA key by checking basic properties
				assert.True(t, got.N.BitLen() > 0)
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
				assert.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}
