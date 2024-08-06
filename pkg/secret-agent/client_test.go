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

//nolint:stylecheck,revive // We want to use package name with underscore.
package secret_agent

import (
	"net"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/secret-agent/connection"
	"github.com/stretchr/testify/require"
)

const (
	testAddress   = ":1234"
	testTimeout   = 10 * time.Second
	testSecretKey = "testSecretKey"
)

func mockTCPServer(address string, handler func(net.Conn)) (net.Listener, error) {
	listener, err := net.Listen(ConnectionTypeTCP, address)
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
	_ = connection.Write(conn, testTimeout, "", testSecretKey)
}

func TestClient_GetSecret(t *testing.T) {
	listener, err := mockTCPServer(testAddress, mockHandler)
	require.NoError(t, err)
	defer listener.Close()

	// Wait for server start.
	time.Sleep(1 * time.Second)

	client, err := NewClient(ConnectionTypeTCP, testAddress, testTimeout, true, nil)
	require.NoError(t, err)

	_, err = client.GetSecret("", testSecretKey)
	require.NoError(t, err)
}
