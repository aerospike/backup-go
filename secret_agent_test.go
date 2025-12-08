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
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
	"github.com/aerospike/backup-go/pkg/secret-agent/connection"
	"github.com/aerospike/backup-go/pkg/secret-agent/models"
	"github.com/stretchr/testify/require"
)

const (
	testSAPort             = 2222
	testSASecretKey        = "secrets:resource:key"
	testSASecretErrPrefix  = "sacred:resource:key"
	testSASecretKeyErr     = "resource:key"
	testSASecretKeyErrLong = "secrets:resource:key:val"
	testCaFile             = "tests/integration/cert_test.pem"
	magic                  = 0x51dec1cc
	testPKey               = `MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDq+ku8oxfSQnUF
4qs8ctSYwtQwgyGViCfO7fnVf+cyIcKhZSCUlqIQPN17pBzUaWLKaLCSvIhehE1N
ETAtbUEgMUbn7R4WGV7N5ACl2mgLh6Rczz5FSSSwrZ/YRSHTsp7oaaKE5bA9S2jY
IKkGMZSGAsh90xVeggDypciI0Pw2aJwed/EXI0PWND2LKut5POJYyHgbxgygp1AC
n1YFH9Vkp06CVcoUj1BqXucuz/qqp8Hj+E2s5P+4JwmGj+hCIJQDve/zQIlyURkO
ZI2hc/QSQVvv4Or1dBR/jJqH6DD0+OmkO0+iZY/8sHJ41yg3H4zU2ZevDuZ2GvDd
qMhShtUVAgMBAAECggEBAKNHsgEuw4rTq0WfsKWclaZhG9lqBZhGuILOUuDMs/be
BsTn5K/bzFnEMZONAouHf6JvBOOyJoCnJp/65aNrW+nm1AKtfk6U6o/fc6PMFKiO
ZOQpDnhOzzQGMiCySUM1x75wSQJYKRMup3gnmcw3/6Dvpino19yIMehq0uJfdiLH
UXh6WrRnJJ1HCOgp+Gjzu1rS2eXzB7LPW3UfYjq6BRzRdjuwiEOEC15w3pnIyIcY
LBg84hImf4B/l4+BAP8gmNW9ky17hwLA/tzOjo8E8eHbJPs3ndxzzCc71j+n7x+o
AIZg6mXFkzKFAR/fN462+ls3sgPVon0LgaNra3ut+jECgYEA9ZeH2W/FeOIUvkBA
eUCrsUs9y2QDnoq5OnRHKfPCWuIRAcJr/tB3GSEZKutF/LfKxK4P2soNQvYsiW7x
RsBgMUeHhrtH4y8mYj/3hQq1DEggf1NEoly2TjIPZ2j+il8QeV0EBCIcN5rvBHuT
6aExlWOu/JZashnGxzKJ2E8+hLsCgYEA9O+cIqJAcXVCspZLVX2B8c8E26nbrBdI
0ufYGNponCH/FrAAE99EURX7Kril7cJ4UzdXcqTp8QUrxClhVPy3jIum/ewLVRN5
freBRjafPY6O0tEzhDp4yPYhzzJwxjmtKkoKhX/KvsJDYGzsxH/yXiT+6L4LHw02
d24ccqMxWG8CgYBqOw1sJEjKrSBD2w8IY8zgd6dXHv/hyCeu/TT7FJFxNnAczrhg
FFQv7n0wb2xqkCWJRbFd9iAeYtWI7RA4hmYVatdYlBHYV0DHJtwuFB+UHG7SJHZ/
tJK26Dh5hpTzzYMWvAFMuGR0OPRCgCHO4QbNk7zRTUgV2ch9yYKOqlhkmQKBgGYE
21KtpAvd3H8IDK66DQLLyGk6EY5XUHTQLnkDl6jYnCg1/IJKb2kar7f2mt4yLu3y
UhElUW+bSMR2u9yrOkRm8pI22+1+pA88nbLCE4ePNjvm+P8tX5vMsP5dMw3Nfivs
FP/P34Ge5nNmSyP5atj9rdMBPR6c4T/TdDPndykvAoGBAKkxR2CDdmpwzR9pdNHc
N3TlUGL8bf3ri9vC1Uwx4HFrqo99pEz4JwXT8VZ/J3KYH9H6hsPfoS9swlBxDjwU
rqDyLDkROLjIJE1InlzrtH9rHGInExFVWbzL1jpCV7GtQsYVdiSxHzNaRB47UGZI
ah87+EsQLgoao6VWDlepN54P`
)

func mockTCPServer(address string, handler func(net.Conn)) (net.Listener, error) {
	listener, err := net.Listen(saClient.ConnectionTypeTCP, address)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go handler(conn)
		}
	}()

	return listener, nil
}

func mockHandler(conn net.Conn) {
	defer conn.Close()
	_, _ = connection.ReadBytes(conn, 10)

	resp := models.Response{
		SecretValue: testPKey,
		Error:       "",
	}
	respJSON, _ := json.Marshal(resp)
	length := len(respJSON)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], uint32(length))

	_, err := conn.Write(append(header, respJSON...))
	if err != nil {
		fmt.Println(err)
	}
}

func TestSecretAgent_isSecret(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		secret string
		result bool
	}{
		{testSASecretKey, true},
		{testSASecretKeyErr, false},
		{"", false},
	}

	for _, tt := range testCases {
		result := isSecret(tt.secret)
		require.Equal(t, tt.result, result)
	}
}

func TestSecretAgent_getResourceKey(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		key        string
		resource   string
		secretKey  string
		errContent string
	}{
		{testSASecretKey, "resource", "key", ""},
		{testSASecretErrPrefix, "", "", "invalid secret key format"},
		{testSASecretKeyErrLong, "", "", "invalid secret key format"},
	}

	for i, tt := range testCases {
		res, sk, err := getResourceKey(tt.key)
		require.Equal(t, tt.resource, res)
		require.Equal(t, tt.secretKey, sk)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}

func TestSecretAgent_getTlSConfig(t *testing.T) {
	t.Parallel()

	filePem := testCaFile
	filePemNotExist := "tests/integration/smth.pem"
	filePemWrong := "tests/integration/pkey_test"

	testCases := []struct {
		config     *SecretAgentConfig
		errContent string
	}{
		{&SecretAgentConfig{CaFile: nil}, ""},
		{&SecretAgentConfig{CaFile: &filePem}, ""},
		{&SecretAgentConfig{CaFile: &filePemNotExist}, "unable to read ca file"},
		{&SecretAgentConfig{CaFile: &filePemWrong}, "nothing to append to ca cert pool"},
	}

	for i, tt := range testCases {
		_, err := getTlSConfig(tt.config)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}

func TestSecretAgent_getSecret(t *testing.T) {
	t.Parallel()

	listener, err := mockTCPServer(fmt.Sprintf(":%d", testSAPort), mockHandler)
	require.NoError(t, err)
	defer listener.Close()

	// Wait for server start.
	time.Sleep(1 * time.Second)

	cfg := testSecretAgentConfig()

	testCases := []struct {
		config     *SecretAgentConfig
		key        string
		errContent string
	}{
		{cfg, testSASecretKey, ""},
		{nil, testSASecretKey, "secret config not initialized"},
		{cfg, testSASecretKeyErr, "invalid secret key format"},
		{cfg, testSASecretErrPrefix, "invalid secret key format"},
		{cfg, testSASecretKeyErrLong, "invalid secret key format"},
	}

	for i, tt := range testCases {
		_, err = getSecret(tt.config, tt.key)
		if tt.errContent != "" {
			require.ErrorContains(t, err, tt.errContent, fmt.Sprintf("case %d", i))
		} else {
			require.NoError(t, err, fmt.Sprintf("case %d", i))
		}
	}
}

func TestParseSecret_ReturnsPlainValueWhenNotSecret(t *testing.T) {
	t.Parallel()

	cfg := &SecretAgentConfig{}
	value, err := ParseSecret(cfg, "plain-value")

	require.NoError(t, err)
	require.Equal(t, "plain-value", value)
}

func TestParseSecret_SecretValueIsResolved(t *testing.T) {
	// We intentionally do NOT call t.Parallel here because we spin up a real TCP listener
	// and want to avoid any flakiness around timing / port reuse.

	// Use mockTCPServer with an ephemeral port.
	listener, err := mockTCPServer("127.0.0.1:0", mockHandler)
	require.NoError(t, err)
	defer listener.Close()

	// Give the server a tiny bit of time to start accepting.
	time.Sleep(20 * time.Millisecond)

	addr := listener.Addr().String()
	connectionType := saClient.ConnectionTypeTCP
	timeoutMs := 1000
	isBase64 := false

	cfg := &SecretAgentConfig{
		Address:            &addr,
		ConnectionType:     &connectionType,
		TimeoutMillisecond: &timeoutMs,
		IsBase64:           &isBase64,
	}

	value, err := ParseSecret(cfg, testSASecretKey)
	require.NoError(t, err)
	require.Equal(t, testPKey, value)
}

func TestParseSecret_SecretAgentError(t *testing.T) {
	t.Parallel()

	// nil config should propagate the NewSecretAgentClient error out of ParseSecret.
	value, err := ParseSecret(nil, testSASecretKey)
	require.ErrorContains(t, err, "secret config not initialized")
	require.Empty(t, value)
}

func TestSecretAgent_getTlSConfig_WithTLSNameAndClientCert(t *testing.T) {
	t.Parallel()

	certPath, keyPath := generateTempCertAndKey(t)
	serverName := "example.com"

	cfg := &SecretAgentConfig{
		CaFile:   &certPath, // also acts as CA here
		TLSName:  &serverName,
		CertFile: &certPath,
		KeyFile:  &keyPath,
	}

	tlsConfig, err := getTlSConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, tlsConfig)
	require.Equal(t, serverName, tlsConfig.ServerName)
	require.NotEmpty(t, tlsConfig.Certificates, "client certificate should be loaded")
}

func TestSecretAgent_getTlSConfig_InvalidClientCert(t *testing.T) {
	t.Parallel()

	// same key file used as both cert and key should cause LoadX509KeyPair to fail
	filePem := testCaFile
	badCertAndKey := "tests/integration/pkey_test"

	cfg := &SecretAgentConfig{
		CaFile:   &filePem,
		CertFile: &badCertAndKey,
		KeyFile:  &badCertAndKey,
	}

	_, err := getTlSConfig(cfg)
	require.ErrorContains(t, err, "failed to load client certificate")
}

func TestNewSecretAgentClient_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewSecretAgentClient(nil)
	require.ErrorContains(t, err, "secret config not initialized")
}

func TestNewSecretAgentClient_TLSConfigError(t *testing.T) {
	t.Parallel()

	// Point CaFile at a non-existing file so getTlSConfig fails and bubbles up.
	addr := "127.0.0.1:0"
	connectionType := saClient.ConnectionTypeTCP
	badCa := "tests/integration/smth.pem"

	cfg := &SecretAgentConfig{
		Address:        &addr,
		ConnectionType: &connectionType,
		CaFile:         &badCa,
	}

	_, err := NewSecretAgentClient(cfg)
	require.ErrorContains(t, err, "unable to read ca file")
}

func TestNewSecretAgentClient_Success(t *testing.T) {
	// As with the ParseSecret success test, avoid t.Parallel since we spin up a real listener.

	listener, err := mockTCPServer("127.0.0.1:0", mockHandler)
	require.NoError(t, err)
	defer listener.Close()

	// Give the server a tiny bit of time to start accepting.
	time.Sleep(20 * time.Millisecond)

	addr := listener.Addr().String()
	connectionType := saClient.ConnectionTypeTCP
	timeoutMs := 1500
	isBase64 := true

	cfg := &SecretAgentConfig{
		Address:            &addr,
		ConnectionType:     &connectionType,
		TimeoutMillisecond: &timeoutMs,
		IsBase64:           &isBase64,
		// CaFile is nil here on purpose to also cover the "no TLS config" path.
	}

	client, err := NewSecretAgentClient(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func generateTempCertAndKey(t *testing.T) (certFile string, keyFile string) {
	t.Helper()

	// Generate RSA private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Self-signed certificate template
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "Test Cert",
			Organization: []string{"Aerospike"},
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	// Write cert to temp file
	dir := t.TempDir()

	certPath := dir + "/cert.pem"
	keyPath := dir + "/key.pem"

	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err)

	// Write key to temp file
	keyOut, err := os.Create(keyPath)
	require.NoError(t, err)
	defer keyOut.Close()

	keyBytes := x509.MarshalPKCS1PrivateKey(priv)
	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes})
	require.NoError(t, err)

	return certPath, keyPath
}
