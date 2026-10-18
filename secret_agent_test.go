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
	"crypto/tls"
	"encoding/base64"
	"testing"

	"github.com/aerospike/tools-common-go/secretagent"
	"github.com/aerospike/tools-common-go/secretagent/secretagenttest"
	"github.com/stretchr/testify/require"
)

const (
	testSAPort            = 2222
	testSAResource        = "resource"
	testSAKey             = "key"
	testSASecretKey       = "secrets:resource:key"
	testSASecretErrPrefix = "sacred:resource:key"
	testSASecretKeyErr    = "resource:key"
	testCaFile            = "tests/integration/cert_test.pem"
	testPKey              = `MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDq+ku8oxfSQnUF
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

func testSecrets() secretagenttest.Secrets {
	return secretagenttest.Secrets{testSAResource: {testSAKey: testPKey}}
}

func testServerConfig(srv *secretagenttest.Server) *SecretAgentConfig {
	connectionType := srv.Network()
	address := srv.Addr()
	cfg := &SecretAgentConfig{ConnectionType: &connectionType, Address: &address}

	if caFile := srv.CAFile(); caFile != "" {
		cfg.CaFile = &caFile
	}

	if certFile, keyFile := srv.ClientCertFile(), srv.ClientKeyFile(); certFile != "" {
		cfg.CertFile, cfg.KeyFile = &certFile, &keyFile
	}

	return cfg
}

func TestParseSecret_ReturnsPlainValueWhenNotSecret(t *testing.T) {
	t.Parallel()

	value, err := ParseSecret(t.Context(), nil, "plain-value")

	require.NoError(t, err)
	require.Equal(t, "plain-value", value)
}

func TestParseSecret_Resolves(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		server func(testing.TB, secretagenttest.Secrets) *secretagenttest.Server
	}{
		{name: "tcp", server: secretagenttest.NewServer},
		{name: "unix", server: secretagenttest.NewUnixServer},
		{name: "tls", server: secretagenttest.NewTLSServer},
		{name: "mutual tls", server: secretagenttest.NewMutualTLSServer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := tt.server(t, testSecrets())

			value, err := ParseSecret(t.Context(), testServerConfig(srv), testSASecretKey)
			require.NoError(t, err)
			require.Equal(t, testPKey, value)
		})
	}
}

func TestParseSecret_References(t *testing.T) {
	t.Parallel()

	srv := secretagenttest.NewServer(t, secretagenttest.Secrets{
		"":         {"key": "no resource"},
		"res:with": {"colon": "colon in resource"},
	})
	cfg := testServerConfig(srv)

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "secrets:key", want: "no resource"},
		{ref: "secrets:res:with:colon", want: "colon in resource"},
	}

	for _, tt := range tests {
		value, err := ParseSecret(t.Context(), cfg, tt.ref)
		require.NoError(t, err, tt.ref)
		require.Equal(t, tt.want, value, tt.ref)
	}
}

func TestParseSecret_Base64(t *testing.T) {
	t.Parallel()

	srv := secretagenttest.NewServer(t, secretagenttest.Secrets{
		testSAResource: {testSAKey: base64.StdEncoding.EncodeToString([]byte("decoded"))},
	})
	cfg := testServerConfig(srv)
	isBase64 := true
	cfg.IsBase64 = &isBase64

	value, err := ParseSecret(t.Context(), cfg, testSASecretKey)
	require.NoError(t, err)
	require.Equal(t, "decoded", value)
}

func TestParseSecret_TLSName(t *testing.T) {
	t.Parallel()

	srv := secretagenttest.NewTLSServer(t, testSecrets())

	tests := []struct {
		tlsName string
		wantErr error
	}{
		{tlsName: "localhost"},
		{tlsName: "example.com", wantErr: ErrSecretAgent},
	}

	for _, tt := range tests {
		cfg := testServerConfig(srv)
		cfg.TLSName = &tt.tlsName

		_, err := ParseSecret(t.Context(), cfg, testSASecretKey)
		if tt.wantErr != nil {
			require.ErrorIs(t, err, tt.wantErr, tt.tlsName)
		} else {
			require.NoError(t, err, tt.tlsName)
		}
	}
}

func TestParseSecret_MinTLSVersion(t *testing.T) {
	t.Parallel()

	pki, err := secretagenttest.GeneratePKI(t.TempDir())
	require.NoError(t, err)

	cert, err := tls.LoadX509KeyPair(pki.ServerCertFile, pki.ServerKeyFile)
	require.NoError(t, err)

	//nolint:gosec // The server caps TLS at 1.2 to reject a client that requires 1.3.
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			_ = conn.(*tls.Conn).HandshakeContext(t.Context())
			_ = conn.Close()
		}
	}()

	connectionType := secretagent.ConnectionTypeTCP
	address := ln.Addr().String()
	minVersion := uint16(tls.VersionTLS13)
	cfg := &SecretAgentConfig{
		ConnectionType: &connectionType,
		Address:        &address,
		CaFile:         &pki.CAFile,
		MinTLSVersion:  &minVersion,
	}

	_, err = ParseSecret(t.Context(), cfg, testSASecretKey)
	require.ErrorIs(t, err, ErrSecretAgent)
	require.ErrorContains(t, err, "protocol version")
}

func TestParseSecret_Errors(t *testing.T) {
	t.Parallel()

	srv := secretagenttest.NewServer(t, testSecrets())
	cfg := testServerConfig(srv)

	tests := []struct {
		name    string
		config  *SecretAgentConfig
		secret  string
		wantErr error
	}{
		{name: "nil config", config: nil, secret: testSASecretKey, wantErr: ErrInvalidConfig},
		{name: "empty key", config: cfg, secret: "secrets:resource:", wantErr: ErrInvalidConfig},
		{name: "unknown secret", config: cfg, secret: "secrets:resource:missing", wantErr: ErrSecretAgent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			value, err := ParseSecret(t.Context(), tt.config, tt.secret)
			require.ErrorIs(t, err, tt.wantErr)
			require.Empty(t, value)
		})
	}
}

func TestGetSecret_Errors(t *testing.T) {
	t.Parallel()

	client, err := NewSecretAgentClient(testSecretAgentConfig())
	require.NoError(t, err)

	tests := []struct {
		name   string
		client *secretagent.Client
		key    string
	}{
		{name: "nil client", client: nil, key: testSASecretKey},
		{name: "missing prefix", client: client, key: testSASecretKeyErr},
		{name: "wrong prefix", client: client, key: testSASecretErrPrefix},
	}

	for _, tt := range tests {
		_, err := getSecret(t.Context(), tt.client, tt.key)
		require.ErrorIs(t, err, ErrInvalidConfig, tt.name)
	}
}

func TestNewSecretAgentClient_Errors(t *testing.T) {
	t.Parallel()

	pki, err := secretagenttest.GeneratePKI(t.TempDir())
	require.NoError(t, err)

	caFile := testCaFile
	missingFile := "tests/integration/smth.pem"
	notACert := "tests/integration/pkey_test"
	unix := secretagent.ConnectionTypeUDS
	unknownType := "udp"
	negativeTimeout := -1

	tests := []struct {
		name    string
		modify  func(*SecretAgentConfig)
		wantErr error
	}{
		{
			name:    "missing ca file",
			modify:  func(c *SecretAgentConfig) { c.CaFile = &missingFile },
			wantErr: ErrInvalidConfig,
		},
		{
			name:    "ca file without certificates",
			modify:  func(c *SecretAgentConfig) { c.CaFile = &notACert },
			wantErr: ErrInvalidConfig,
		},
		{
			name: "invalid client certificate",
			modify: func(c *SecretAgentConfig) {
				c.CaFile, c.CertFile, c.KeyFile = &caFile, &notACert, &notACert
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "client certificate without ca file",
			modify: func(c *SecretAgentConfig) {
				c.CertFile, c.KeyFile = &pki.ClientCertFile, &pki.ClientKeyFile
			},
			wantErr: ErrInvalidConfig,
		},
		{
			name: "tls over unix socket",
			modify: func(c *SecretAgentConfig) {
				c.ConnectionType, c.CaFile = &unix, &caFile
			},
			wantErr: ErrUnsupported,
		},
		{
			name:    "unknown connection type",
			modify:  func(c *SecretAgentConfig) { c.ConnectionType = &unknownType },
			wantErr: ErrInvalidConfig,
		},
		{
			name:    "negative timeout",
			modify:  func(c *SecretAgentConfig) { c.TimeoutMillisecond = &negativeTimeout },
			wantErr: ErrInvalidConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testSecretAgentConfig()
			tt.modify(cfg)

			_, err := NewSecretAgentClient(cfg)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestNewSecretAgentClient_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewSecretAgentClient(nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}
