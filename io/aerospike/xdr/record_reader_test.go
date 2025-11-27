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
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/backup-go/io/aerospike/xdr/mocks"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testDC                = "DCTest"
	testNamespace         = "test"
	testRewind            = "all"
	testCurrentHost       = ":8081"
	testCurrentHost2      = ":8082"
	testCurrentHost3      = ":8083"
	testCurrentHost4      = ":8084"
	testCurrentHost5      = ":8085"
	testInfoPollingPeriod = 1 * time.Second
	testStartTimeout      = 10 * time.Second
)

func testTCPConfig(host string) *TCPConfig {
	cfg := newDefaultTCPConfig()
	// Remap address to the specified host
	cfg.Address = host
	return cfg
}

func testRecordReaderConfig(host ...string) *RecordReaderConfig {
	hostToUse := testCurrentHost
	if len(host) > 0 {
		hostToUse = host[0]
	}
	return NewRecordReaderConfig(
		testDC,
		testNamespace,
		testRewind,
		hostToUse,
		testTCPConfig(hostToUse),
		testInfoPollingPeriod,
		testStartTimeout,
		0,
		true,
	)
}

func newInfoMock(t *testing.T) infoCommander {
	t.Helper()
	ic := mocks.NewMockinfoCommander(t)
	ic.On("StartXDR", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ic.On("GetStats", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(asinfo.Stats{
			Recoveries:        0,
			RecoveriesPending: 1,
			Lag:               0,
		}, nil)
	ic.On("GetNodesNames").
		Return([]string{"node1"})

	return ic
}

func TestRecordReader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		// Level: slog.LevelDebug,
	}))
	ic := newInfoMock(t)

	r, err := NewRecordReader(ctx, ic, testRecordReaderConfig(testCurrentHost2), logger)
	require.NoError(t, err)

	var counter atomic.Uint64
	go func() {
		for {
			token, err := r.Read(ctx)
			switch {
			case err == nil:
				require.Equal(t, testKeyString, token.Key.String())
				counter.Add(1)
				continue
			case errors.Is(err, io.EOF):
				return
			default:
				require.NoError(t, err)
			}
		}
	}()

	time.Sleep(1 * time.Second)

	tcpClient, err := newTCPClient(testCurrentHost2)
	require.NoError(t, err)

	go func() {
		for i := 0; i < 3; i++ {
			msg, err := newMessage(testMessageB64)
			require.NoError(t, err)
			err = sendMessage(tcpClient, msg)
			require.NoError(t, err)
		}
	}()

	time.Sleep(3 * time.Second)

	r.Close()

	require.Equal(t, uint64(3), counter.Load())
}

func TestRecordReaderCloseNotRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	ic := mocks.NewMockinfoCommander(t)

	r, err := NewRecordReader(ctx, ic, testRecordReaderConfig(testCurrentHost3), logger)
	require.NoError(t, err)

	r.Close()
}

func TestRecordReaderServeNoNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	ic := mocks.NewMockinfoCommander(t)
	ic.On("GetNodesNames").Return([]string{})

	r, err := NewRecordReader(ctx, ic, testRecordReaderConfig(testCurrentHost4), logger)
	require.NoError(t, err)

	r.serve()

	require.False(t, r.isRunning.Load())
}

func TestRecordReaderWatchNodes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	ic := mocks.NewMockinfoCommander(t)
	ic.On("BlockMRTWrites", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	hostPort := testCurrentHost5
	r, err := NewRecordReader(ctx, ic, testRecordReaderConfig(hostPort), logger)
	require.NoError(t, err)

	node1 := NewNodeReader(ctx, "node1", ic, testRecordReaderConfig(hostPort), r.nodesRecovered, logger)
	node2 := NewNodeReader(ctx, "node2", ic, testRecordReaderConfig(hostPort), r.nodesRecovered, logger)

	r.anMu.Lock()
	r.activeNodes = []*NodeReader{node1, node2}
	r.anMu.Unlock()

	go r.watchNodes()

	r.nodesRecovered <- struct{}{}
	r.nodesRecovered <- struct{}{}

	time.Sleep(100 * time.Millisecond)

	require.True(t, node1.mrtWritesStopped.Load())
	require.True(t, node2.mrtWritesStopped.Load())
}
