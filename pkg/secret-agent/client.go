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

//nolint:revive // We want to use package name with underscore.
package secret_agent

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/aerospike/backup-go/pkg/secret-agent/connection"
	"github.com/segmentio/asm/base64"
)

const (
	// ConnectionTypeTCP connection type for TCP.
	ConnectionTypeTCP = "tcp"
	// ConnectionTypeUDS connection type for unix socket.
	ConnectionTypeUDS = "unix"
)

// Client represents the client to communicate with Aerospike Secret Agent.
type Client struct {
	// tlsConfig contains the TLS configuration for secure connection.go over TCP.
	tlsConfig *tls.Config
	// connectionType describes connection.go type. Use `ConnectionTypeUDS` and
	// `ConnectionTypeTCP` constants to define.
	connectionType string
	// address contains the address of the Aerospike Secret Agent.
	// for `ConnectionTypeTCP` it will be host + port, e.g.: "127.0.0.1:3005"
	// for `ConnectionTypeUDS` it will be path to unix socket, e.g.: "/tmp/test.sock"
	address string
	// timeout contains connection timeout for read and write operations.
	timeout time.Duration
	// isBase64 determines whether we need to base64 decode the response returned
	// by the secret agent.
	isBase64 bool
}

// NewClient returns a new Aerospike Secret Agent client.
func NewClient(connectionType, address string, timeout time.Duration, isBase64 bool,
	tlsConfig *tls.Config,
) (*Client, error) {
	if tlsConfig != nil && connectionType != ConnectionTypeTCP {
		return nil, fmt.Errorf("tls connection type %s is not supported", connectionType)
	}

	return &Client{
		connectionType: connectionType,
		address:        address,
		timeout:        timeout,
		isBase64:       isBase64,
		tlsConfig:      tlsConfig,
	}, nil
}

// GetSecret performs a request to the Aerospike Secret Agent. If the key is found
// in the external service, the corresponding value will be returned. Otherwise,
// an empty value and an error will be returned.
func (c *Client) GetSecret(resource, secretKey string) (string, error) {
	conn, err := connection.Get(c.connectionType, c.address, c.timeout, c.tlsConfig)
	if err != nil {
		return "", fmt.Errorf("failed to connect to secret agent: %w", err)
	}

	defer conn.Close()

	//nolint:gocritic // inline if
	if err = connection.Write(conn, c.timeout, resource, secretKey); err != nil {
		return "", err
	}

	response, err := connection.Read(conn, c.timeout)
	if err != nil {
		return "", err
	}

	// If the secret agent is configured to encode all responses to base64,
	// we need to decode the response.
	if c.isBase64 {
		var decoded []byte

		decoded, err = base64.StdEncoding.DecodeString(response)
		if err != nil {
			return "", err
		}

		return string(decoded), nil
	}

	return response, nil
}
