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

package aerospike

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testAddress = "localhost:1025"
	testTimeout = 1 * time.Second
	testPayload = "payload"
)

func Test_TCPServer(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	resultChan := make(chan *models.XDRToken)
	errChan := make(chan error, 1)

	tcpSrv := NewTCPServer(testAddress, testTimeout, nil, resultChan, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := tcpSrv.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for the server to start.
	time.Sleep(1 * time.Second)

	conn, err := net.Dial("tcp", testAddress)
	require.NoError(t, err)
	defer conn.Close()

	data := []byte(testPayload)
	length := len(data)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(length))
	_, err = conn.Write(append(header, data...))
	require.NoError(t, err)

	result := <-resultChan
	require.Equal(t, result.Payload, []byte(testPayload))
}
