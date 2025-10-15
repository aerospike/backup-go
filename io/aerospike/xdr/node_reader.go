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
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/pkg/asinfo"
)

const (
	errDcNotFound    = "DC not found"
	getStatsAttempts = 5
)

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
	isRecovered    atomic.Bool

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
		r.ctx,
		r.nodeName,
		r.config.dc,
		r.config.currentHostPort,
		r.config.namespace,
		r.config.rewind,
		r.config.maxThroughput,
		r.config.xdrForward,
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

	var stateSent bool

	time.Sleep(statsPollingDelay)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			stats, err := r.getStats()
			if err != nil {
				r.logger.Error("failed to get stats",
					slog.Any("error", err),
				)

				return
			}

			r.logger.Debug("got stats",
				slog.Any("stats", stats),
			)

			if stats.RecoveriesPending != 0 {
				// Recovery in progress.
				continue
			}

			// Recovery finished. Notify the reader to stop MRT writes.
			if !stateSent {
				r.nodesRecovered <- struct{}{}
				r.isRecovered.Store(true)

				stateSent = true
			}

			if !r.mrtWritesStopped.Load() {
				// Wait for all the nodes.
				continue
			}

			// Set once.
			if r.checkpoint == 0 {
				r.checkpoint = time.Now().Unix()
			}

			// Convert lag from citrus leaf epoch.
			clLag := cltime.NewCLTime(stats.Lag)
			unixLag := clLag.Unix()

			if r.checkpoint-unixLag < 0 || stats.Lag == 0 {
				// Run MRT writes.
				if err = r.infoClient.UnBlockMRTWrites(r.ctx, r.nodeName, r.config.namespace); err != nil {
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

	// If we close because of error, we must remove it from observer.
	if !r.isRecovered.Load() {
		r.nodesRecovered <- struct{}{}
	}

	if err := r.infoClient.StopXDR(r.ctx, r.nodeName, r.config.dc); err != nil {
		r.logger.Error("failed to remove xdr config",
			slog.Any("error", err))
	}

	// If mrt was stopped.
	if r.mrtWritesStopped.Load() {
		if err := r.infoClient.UnBlockMRTWrites(r.ctx, r.nodeName, r.config.namespace); err != nil {
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
	if err := r.infoClient.BlockMRTWrites(r.ctx, r.nodeName, r.config.namespace); err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	return nil
}

func (r *NodeReader) getStats() (*asinfo.Stats, error) {
	var (
		errChain error
		delay    time.Duration
	)

	handleError := func(err error, description string) {
		if !errors.Is(errChain, err) {
			errChain = errors.Join(errChain,
				fmt.Errorf("%s: %w", description, err))
		}

		delay += time.Second
	}

	for retries := 0; retries < getStatsAttempts; retries++ {
		time.Sleep(delay)

		stats, err := r.infoClient.GetStats(r.ctx, r.nodeName, r.config.dc, r.config.namespace)

		switch {
		case err == nil:
			return &stats, nil
		case strings.Contains(err.Error(), errDcNotFound):
			r.logger.Warn("failed to get stats, try to restart xdr", slog.Any("error", err))
			// Try to restart XDR.
			if err = r.infoClient.StartXDR(
				r.ctx,
				r.nodeName,
				r.config.dc,
				r.config.currentHostPort,
				r.config.namespace,
				r.config.rewind,
				r.config.maxThroughput,
				r.config.xdrForward,
			); err != nil {
				handleError(err, "failed to restart xdr")
			}
			// After successful restart of XDR we should wait, until xdr will restart.
			time.Sleep(statsPollingDelay)
		default:
			handleError(err, "failed to get stats")
		}
	}

	return nil, fmt.Errorf("failed to get stats for node %s after %d attempts: %w",
		r.nodeName, getStatsAttempts, errChain)
}
