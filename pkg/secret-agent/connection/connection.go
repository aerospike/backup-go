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

package connection

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/aerospike/backup-go/pkg/secret-agent/models"
)

// magic const is taken from the Secret Agent service. It is used
// by the service to validate TCP requests.
const magic = 0x51dec1cc

//go:generate mockery
type connector interface {
	Write(b []byte) (n int, err error)
	Read(b []byte) (n int, err error)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// Get returns a connector according to initialized params.
func Get(
	connectionType, address string,
	timeout time.Duration,
	tlsConfig *tls.Config,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if tlsConfig != nil {
		return tls.DialWithDialer(dialer, connectionType, address, tlsConfig)
	}

	return dialer.Dial(connectionType, address)
}

// Write forms and executes a request to the secret agent.
func Write(conn connector, timeout time.Duration, resource, secretKey string) error {
	// Setting writing timeout.
	deadline := time.Now().Add(timeout)
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	msg := models.Request{
		Resource:  resource,
		SecretKey: secretKey,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Adding headers.
	length := len(data)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], magic)
	binary.BigEndian.PutUint32(header[4:], uint32(length))

	// Sending message.
	_, err = conn.Write(append(header, data...))
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	return nil
}

// Read reads and parse response from secret agent.
func Read(conn connector, timeout time.Duration) (string, error) {
	// Setting reading timeout.
	deadline := time.Now().Add(timeout)
	if err := conn.SetReadDeadline(deadline); err != nil {
		return "", fmt.Errorf("failed to set read deadline: %w", err)
	}
	// Reading headers.
	header, err := ReadBytes(conn, 8)
	if err != nil {
		return "", fmt.Errorf("failed to read header: %w", err)
	}

	// Checking headers.
	receivedMagic := binary.BigEndian.Uint32(header[:4])
	length := binary.BigEndian.Uint32(header[4:])

	if receivedMagic != magic {
		return "", fmt.Errorf("invalid magic number: %x", receivedMagic)
	}

	// Reading body.
	body, err := ReadBytes(conn, int(length))
	if err != nil {
		return "", fmt.Errorf("failed to read header: %w", err)
	}

	var res models.Response
	if err = json.Unmarshal(body, &res); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if res.Error != "" {
		return "", fmt.Errorf("%s", res.Error)
	}

	return res.SecretValue, nil
}

func ReadBytes(conn connector, length int) ([]byte, error) {
	buffer := make([]byte, length)
	total := 0

	for total < length {
		n, err := conn.Read(buffer[total:])
		if err != nil {
			return nil, err
		}

		total += n
	}

	return buffer, nil
}
