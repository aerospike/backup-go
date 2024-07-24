//nolint:stylecheck,revive // We want to use package name with underscore.
package secret_agent

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/aerospike/backup-go/pkg/secret-agent/connection"
)

const (
	// ConnectionTypeTCP  connection type for TCP.
	ConnectionTypeTCP = "tcp"
	// ConnectionTypeUDS  connection type for unix socket.
	ConnectionTypeUDS = "unix"
)

// Client initialize client for connecting to aerospike secret agent.
type Client struct {
	// tlsConfig contains tls config for secure connection.go over TCP.
	tlsConfig *tls.Config
	// connectionType describes connection.go type. Use `ConnectionTypeUDS` and `ConnectionTypeTCP` constants to define.
	connectionType string
	// address contains address of aerospike secret agent.
	// for `ConnectionTypeTCP` it will be host + port, e.g.: "127.0.0.1:3005"
	// for `ConnectionTypeUDS` it will be path to unix socket, e.g.: "/tmp/test.sock"
	address string
	// timeout contains timeouts for connection, read and write operations.
	timeout time.Duration
	// isBase64 contains flag, do we need to decode keys from secret agent or not.
	// If agent is configured to return base64 encoded results.
	isBase64 bool
}

// NewClient returns new aerospike secret agent client.
func NewClient(connectionType, address string, timeout time.Duration, isBase64 bool, tlsConfig *tls.Config,
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

// GetSecret performs request to aerospike secret agent.
func (c *Client) GetSecret(resource, secretKey string) (string, error) {
	conn, err := connection.Get(c.connectionType, c.address, c.timeout, c.tlsConfig)
	if err != nil {
		return "", fmt.Errorf("failed to connect to secret agent: %w", err)
	}

	defer conn.Close()

	//nolint:gocritic // I want to write if inline.
	if err = connection.Write(conn, c.timeout, resource, secretKey); err != nil {
		return "", err
	}

	response, err := connection.Read(conn, c.timeout)
	if err != nil {
		return "", err
	}

	// If secret agent configured to encrypt all responses to base64, we decrypt it.
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
