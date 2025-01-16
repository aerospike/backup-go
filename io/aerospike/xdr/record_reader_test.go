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
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/io/aerospike/xdr/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testDC               = "DCTest"
	testNamespace        = "test"
	testRewind           = "all"
	testCurrentHost      = ":8081"
	testInfoPolingPeriod = 1 * time.Second
)

func testTCPConfig() *TCPConfig {
	cfg := NewDefaultTCPConfig()
	// Remap address, as 8080 is used by another test.
	cfg.Address = testCurrentHost
	return cfg
}

func testRecordReaderConfig() *RecordReaderConfig {
	return NewRecordReaderConfig(
		testDC,
		testNamespace,
		testRewind,
		testCurrentHost,
		testTCPConfig(),
		testInfoPolingPeriod,
	)
}

func newInfoMock(t *testing.T) infoCommander {
	t.Helper()
	ic := mocks.NewMockinfoCommander(t)
	ic.On("StartXDR", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ic.On("GetStats", mock.Anything, mock.Anything).
		Return(asinfo.Stats{
			Recoveries:        1,
			RecoveriesPending: 1,
			Lag:               time.Now().UnixNano(),
		}, nil)
	ic.On("StopXDR", mock.Anything).
		Return(nil)
	ic.On("BlockMRTWrites", mock.Anything).
		Return(nil)
	ic.On("UnBlockMRTWrites", mock.Anything).
		Return(nil)

	return ic
}

func TestRecordReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ic := newInfoMock(t)

	r, err := NewRecordReader(ctx, ic, testRecordReaderConfig(), logger)
	require.NoError(t, err)

	// Start to read messages.
	var counter int
	go func() {
		for {
			token, err := r.Read()
			switch {
			case err == nil:
				require.Equal(t, testKeyString, token.Key.String())
				counter++
				continue
			case errors.Is(err, io.EOF):
				return
			default:
				require.NoError(t, err)
			}
		}
	}()

	// Wait for the TCP server to start.
	time.Sleep(1 * time.Second)

	tcpClient, err := newTCPClient(testCurrentHost)
	require.NoError(t, err)

	// Sending messages.
	go func() {
		// Send 3 valid messages.
		for i := 0; i < 3; i++ {
			msg, err := newMessage(testMessageB64)
			require.NoError(t, err)
			err = sendMessage(tcpClient, msg)
			require.NoError(t, err)
		}
	}()

	time.Sleep(3 * time.Second)

	r.Close()

	require.Equal(t, 3, counter)
}
