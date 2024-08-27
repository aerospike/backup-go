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
	"fmt"
	"os"
	"testing"
	"time"

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
		_, err = ReadPrivateKey(tt.encPolicy, tt.saConfig)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}
