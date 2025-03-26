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
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/internal/asinfo"
	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
)

const defaultStartRetries = 3

// NodeReader track each node.
type NodeReader struct {
	ctx context.Context
	// Name of the node.
	nodeName string
	// Info client to start and stop XDR, also to get current state.
	infoClient infoCommander
	// Records reader config.
	config *RecordReaderConfig
	// Time when recovery finished.
	checkpoint int64
	// To check if mrt is stopped.
	mrtWritesStopped atomic.Bool

	nodesRecovered chan struct{}

	logger *slog.Logger
}

func NewNodeReader(
	ctx context.Context,
	nodeName string,
	infoClient infoCommander,
	config *RecordReaderConfig,
	nodesRecovered chan struct{},
	logger *slog.Logger,
) *NodeReader {
	logger = logger.With(
		slog.String("node", nodeName),
		slog.String("dc", config.dc),
		slog.String("namespace", config.namespace),
	)

	return &NodeReader{
		nodeName:       nodeName,
		ctx:            ctx,
		infoClient:     infoClient,
		config:         config,
		nodesRecovered: nodesRecovered,
		logger:         logger,
	}
}

func (r *NodeReader) Run() error {
	// Create XDR config.
	if err := r.infoClient.StartXDR(
		r.nodeName,
		r.config.dc,
		r.config.currentHostPort,
		r.config.namespace,
		r.config.rewind,
		r.config.maxThroughput,
	); err != nil {
		return fmt.Errorf("failed to create xdr config for node %s: %w", r.nodeName, err)
	}

	r.logger.Debug("created xdr config",
		slog.String("hostPort", r.config.currentHostPort),
		slog.String("rewind", r.config.rewind),
		slog.Int("throughput", r.config.maxThroughput),
	)

	r.serve()

	return nil
}

func (r *NodeReader) serve() {
	ticker := time.NewTicker(r.config.infoPolingPeriod)
	defer ticker.Stop()
	defer r.close()

	var (
		stateSent    bool
		startRetries int
	)

	time.Sleep(statsPollingDelay)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			stats, err := r.getStats(&startRetries)
			if err != nil {
				return
			}

			if stats == nil {
				continue
			}

			r.logger.Debug("got stats",
				slog.Any("stats", stats),
			)

			if stats.RecoveriesPending != 0 {
				// Recovery in progress.
				continue
			}

			// Recovery finished.
			if !stateSent {
				r.nodesRecovered <- struct{}{}

				stateSent = true
			}

			if !r.mrtWritesStopped.Load() {
				// Wait for all the nodes.
				continue
			}

			// set once.
			if r.checkpoint == 0 {
				r.checkpoint = time.Now().Unix()
			}

			// Convert lag from citrus leaf epoch.
			clLag := cltime.NewCLTime(stats.Lag)
			unixLag := clLag.Unix()

			if r.checkpoint-unixLag < 0 || stats.Lag == 0 {
				// Run MRT writes.
				if err = r.infoClient.UnBlockMRTWrites(r.nodeName, r.config.namespace); err != nil {
					r.logger.Error("failed to unblock mrt writes",
						slog.Any("error", err))
				}

				r.logger.Debug("mrt unblocked")
				r.mrtWritesStopped.Store(false)

				// Correct exit from routine.
				return
			}
		}
	}
}

func (r *NodeReader) close() {
	r.logger.Debug("closing aerospike node record reader")

	if err := r.infoClient.StopXDR(r.nodeName, r.config.dc); err != nil {
		r.logger.Error("failed to remove xdr config",
			slog.Any("error", err))
	}

	// If mrt was stopped.
	if r.mrtWritesStopped.Load() {
		if err := r.infoClient.UnBlockMRTWrites(r.nodeName, r.config.namespace); err != nil {
			r.logger.Error("failed to unblock mrt writes", slog.Any("error", err))
		}
		// Only after successful unblocking.
		r.mrtWritesStopped.Store(false)
	}

	r.logger.Debug("closed aerospike node record reader")
}

func (r *NodeReader) BlockMrt() error {
	r.mrtWritesStopped.Store(true)
	// Stop MRT writes in this checkpoint.
	if err := r.infoClient.BlockMRTWrites(r.nodeName, r.config.namespace); err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	return nil
}

func (r *NodeReader) getStats(startRetries *int) (*asinfo.Stats, error) {
	stats, err := r.infoClient.GetStats(r.nodeName, r.config.dc, r.config.namespace)
	if err == nil {
		return &stats, nil
	}

	if strings.Contains(err.Error(), "ERROR:2:DC not found") {
		// Only attempt restart if we haven't exceeded retry limit
		if *startRetries >= defaultStartRetries {
			r.logger.Error("failed to recreate xdr config for node, shut down",
				slog.Int("try", *startRetries),
				slog.String("node", r.nodeName),
				slog.Any("error", err))

			return nil, err
		}

		// Try to restart XDR
		if err := r.infoClient.StartXDR(
			r.nodeName,
			r.config.dc,
			r.config.currentHostPort,
			r.config.namespace,
			r.config.rewind,
			r.config.maxThroughput,
		); err != nil {
			r.logger.Warn("failed to recreate xdr config for node",
				slog.Int("try", *startRetries),
				slog.String("node", r.nodeName),
				slog.Any("error", err))

			*startRetries++

			return nil, nil
		}

		// XDR restarted successfully, but we still need to return nil,nil to signal caller to retry
		return nil, nil
	}

	// Any other error
	r.logger.Warn("failed to get xdr stats", slog.Any("error", err))

	return nil, err
}
