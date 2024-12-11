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

package xdr

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testMessageB64      = "FhABEAAAAAAAAgAnjQAAAAAAAAUAAQAAAAsAc291cmNlLW5zMQAAABUE/+Ptyjj06wW9zx0AnxOmq45xJzsAAAAFAXNldDEAAAAKAgEAAAAAAAADCQAAAAkOAAAAbcndaZgAAAAUAgMAAWF6enp6enp6enp6enp6eno="
	testErrorMessageB64 = "ZXJyb3IgbWVzc2FnZQ=="
	testHost            = ":8080"
	testTimeOut         = 1 * time.Second
	testKeyString       = "source-ns1:set1:777:ff e3 ed ca 38 f4 eb 05 bd cf 1d 00 9f 13 a6 ab 8e 71 27 3b"
)

func TestTCPServer(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := NewDefaultTCPConfig()

	srv := NewTCPServer(
		cfg,
		logger,
	)

	ctx := context.Background()

	results, err := srv.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Wait for server to start.
	time.Sleep(3 * time.Second)

	client, err := newTCPClient()
	require.NoError(t, err)

	go func() {
		// Send 3 valid messages.
		for i := 0; i < 3; i++ {
			msg, err := newMessage(testMessageB64)
			require.NoError(t, err)
			err = sendMessage(client, msg)
			require.NoError(t, err)
		}
		// Send one invalid message.
		msg, err := newMessage(testErrorMessageB64)
		require.NoError(t, err)
		err = sendMessage(client, msg)
		require.NoError(t, err)
	}()

	go func() {
		time.Sleep(5 + time.Second)
		srv.Stop()
	}()

	require.Equal(t, int32(1), srv.GetActiveConnections())

	var counter int
	for result := range results {
		require.Equal(t, testKeyString, result.Key.String())
		counter++
	}
	require.Equal(t, 3, counter)
}

func newTCPClient() (net.Conn, error) {
	dialer := &net.Dialer{Timeout: testTimeOut}
	return dialer.Dial("tcp", testHost)
}

func sendMessage(conn net.Conn, message []byte) error {
	deadline := time.Now().Add(testTimeOut)
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	if _, err := conn.Write(message); err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	return nil
}

func newMessage(message string) ([]byte, error) {
	body, err := base64.StdEncoding.DecodeString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}

	return NewPayload(body), nil
}
