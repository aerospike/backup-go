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
	"log/slog"
	"os"
	"testing"

	"github.com/aerospike/backup-go/io/aerospike/xdr/mocks"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNodeReader_Run(t *testing.T) {
	t.Parallel()
	t.Run("StartXDRError", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("StartXDR", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("start xdr error"))

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		err := nr.Run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create xdr config")
	})
}

func TestNodeReader_BlockMrt(t *testing.T) {
	t.Parallel()
	t.Run("SuccessfulBlock", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("BlockMRTWrites", mock.Anything, mock.Anything).Return(nil)

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		err := nr.BlockMrt()
		require.NoError(t, err)
		require.True(t, nr.mrtWritesStopped.Load())
	})

	t.Run("BlockError", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("BlockMRTWrites", mock.Anything, mock.Anything).
			Return(errors.New("block error"))

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		err := nr.BlockMrt()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to block mrt writes")
		require.True(t, nr.mrtWritesStopped.Load())
	})
}

func TestNodeReader_GetStats(t *testing.T) {
	t.Parallel()
	t.Run("SuccessfulGetStats", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("GetStats", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(asinfo.Stats{
				Recoveries:        1,
				RecoveriesPending: 0,
				Lag:               10,
			}, nil)

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		stats, err := nr.getStats()
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.Recoveries)
		require.Equal(t, int64(0), stats.RecoveriesPending)
		require.Equal(t, int64(10), stats.Lag)
	})

	t.Run("GetStatsError", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("GetStats", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(asinfo.Stats{}, errors.New("get stats error"))

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		_, err := nr.getStats()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get stats")
	})

	t.Run("DCNotFoundError", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)

		ic.On("GetStats", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(asinfo.Stats{}, errors.New("DC not found")).Once()

		ic.On("GetStats", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(asinfo.Stats{
				Recoveries:        1,
				RecoveriesPending: 0,
				Lag:               10,
			}, nil).Once()

		ic.On("StartXDR", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		stats, err := nr.getStats()
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.Recoveries)
		require.Equal(t, int64(0), stats.RecoveriesPending)
		require.Equal(t, int64(10), stats.Lag)
	})

	t.Run("MaxRetriesExceeded", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)

		ic.On("GetStats", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(asinfo.Stats{}, errors.New("persistent error"))

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		_, err := nr.getStats()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get stats for node")
		require.Contains(t, err.Error(), "after 5 attempts")
	})
}

func TestNodeReader_Close(t *testing.T) {
	t.Parallel()
	t.Run("CloseWithoutRecovery", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("StopXDR", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		nr.close()

		select {
		case <-nodesRecovered:
			// Success
		default:
			t.Fatal("nodesRecovered channel should have received a message")
		}
	})

	t.Run("CloseWithRecovery", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("StopXDR", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		nr.isRecovered.Store(true)

		nr.close()

		select {
		case <-nodesRecovered:

			t.Fatal("nodesRecovered channel should not have received a message")
		default:
			// Success
		}
	})

	t.Run("CloseWithMrtStopped", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		ic := mocks.NewMockinfoCommander(t)
		ic.On("StopXDR", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		ic.On("UnBlockMRTWrites", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		nodesRecovered := make(chan struct{}, 1)
		nr := NewNodeReader(
			ctx,
			"node1",
			ic,
			testRecordReaderConfig(),
			nodesRecovered,
			logger,
		)

		nr.mrtWritesStopped.Store(true)

		nr.close()

		require.False(t, nr.mrtWritesStopped.Load())
	})
}
