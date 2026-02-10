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

//nolint:revive,nolintlint // We want to use package name with underscore.
package secret_agent

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/secret-agent/connection"
	"github.com/aerospike/backup-go/pkg/secret-agent/models"
	"github.com/stretchr/testify/require"
)

const (
	testAddress   = ":1111"
	testTimeout   = 10 * time.Second
	testSecretKey = "testSecretKey"
	magic         = 0x51dec1cc
	testPKey      = `MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDq+ku8oxfSQnUF
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
	testBase64Value = "dGVzdFZhbHVl" // base64 encoded "testValue"
)

func mockTCPServer(address string, handler func(net.Conn)) (net.Listener, error) {
	var lc net.ListenConfig
	listener, err := lc.Listen(context.Background(), ConnectionTypeTCP, address)
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
	defer sneakyClose(conn)
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

func mockBase64Handler(conn net.Conn) {
	defer sneakyClose(conn)
	_, _ = connection.ReadBytes(conn, 10)

	resp := models.Response{
		SecretValue: testBase64Value,
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

func mockErrorHandler(conn net.Conn) {
	defer sneakyClose(conn)
	_, _ = connection.ReadBytes(conn, 10)

	resp := models.Response{
		SecretValue: "",
		Error:       "test error",
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

func mockInvalidBase64Handler(conn net.Conn) {
	defer sneakyClose(conn)
	_, _ = connection.ReadBytes(conn, 10)

	resp := models.Response{
		SecretValue: "invalid-base64",
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

func TestNewClient(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		client, err := NewClient(ConnectionTypeTCP, testAddress, testTimeout, true, nil)
		require.NoError(t, err)
		require.NotNil(t, client)
		require.Equal(t, ConnectionTypeTCP, client.connectionType)
		require.Equal(t, testAddress, client.address)
		require.Equal(t, testTimeout, client.timeout)
		require.True(t, client.isBase64)
		require.Nil(t, client.tlsConfig)
	})

	t.Run("Error with TLS and non-TCP", func(t *testing.T) {
		//nolint:gosec // For test we can use default unsecured TLS version.
		tlsConfig := &tls.Config{}
		client, err := NewClient(ConnectionTypeUDS, testAddress, testTimeout, true, tlsConfig)
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "tls connection type unix is not supported")
	})
}

func TestClient_GetSecret(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		listener, err := mockTCPServer(testAddress, mockHandler)
		require.NoError(t, err)
		defer sneakyClose(listener)

		// Wait for server start.
		time.Sleep(1 * time.Second)

		client, err := NewClient(ConnectionTypeTCP, testAddress, testTimeout, false, nil)
		require.NoError(t, err)

		secret, err := client.GetSecret(t.Context(), "", testSecretKey)
		require.NoError(t, err)
		require.Equal(t, testPKey, secret)
	})

	t.Run("Success with Base64 Decoding", func(t *testing.T) {
		listener, err := mockTCPServer(":1112", mockBase64Handler)
		require.NoError(t, err)
		defer sneakyClose(listener)

		// Wait for server start.
		time.Sleep(1 * time.Second)

		client, err := NewClient(ConnectionTypeTCP, ":1112", testTimeout, true, nil)
		require.NoError(t, err)

		secret, err := client.GetSecret(t.Context(), "", testSecretKey)
		require.NoError(t, err)
		require.Equal(t, "testValue", secret)
	})

	t.Run("Error from Server", func(t *testing.T) {
		listener, err := mockTCPServer(":1113", mockErrorHandler)
		require.NoError(t, err)
		defer sneakyClose(listener)

		// Wait for server start.
		time.Sleep(1 * time.Second)

		client, err := NewClient(ConnectionTypeTCP, ":1113", testTimeout, false, nil)
		require.NoError(t, err)

		_, err = client.GetSecret(t.Context(), "", testSecretKey)
		require.Error(t, err)
		require.Contains(t, err.Error(), "test error")
	})

	t.Run("Invalid Base64", func(t *testing.T) {
		listener, err := mockTCPServer(":1114", mockInvalidBase64Handler)
		require.NoError(t, err)
		defer sneakyClose(listener)

		// Wait for server start.
		time.Sleep(1 * time.Second)

		client, err := NewClient(ConnectionTypeTCP, ":1114", testTimeout, true, nil)
		require.NoError(t, err)

		_, err = client.GetSecret(t.Context(), "", testSecretKey)
		require.Error(t, err)
		require.Contains(t, err.Error(), "illegal base64")
	})

	t.Run("Connection Error", func(t *testing.T) {
		client, err := NewClient(ConnectionTypeTCP, "invalid-address", testTimeout, false, nil)
		require.NoError(t, err)

		_, err = client.GetSecret(t.Context(), "", testSecretKey)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to connect to secret agent")
	})
}

func sneakyClose(c io.Closer) {
	_ = c.Close()
}
