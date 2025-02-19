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
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/internal/asinfo"
	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/models"
)

// After that delay, we start to poll stats from db.
const statsPollingDelay = 3 * time.Second

// RecordReaderConfig represents the configuration for getting Aerospike records trough XDR.
type RecordReaderConfig struct {
	// dc name that will be created for xdr.
	dc string
	// namespace to backup.
	namespace string
	// rewind to start reading records from this point.
	// Can be: "all" or number.
	rewind string
	// Current host port, so XDR will send data to this address.
	currentHostPort string
	// TCP server config to serve XDR backup.
	tcpConfig *TCPConfig
	// infoPolingPeriod how often stats will be requested.
	// To measure recovery state and lag.
	infoPolingPeriod time.Duration
	// Timeout for reading the first message after XDR start.
	startTimeout time.Duration
}

// NewRecordReaderConfig creates a new RecordReaderConfig.
func NewRecordReaderConfig(
	dc string,
	namespace string,
	rewind string,
	currentHostPort string,
	tcpConfig *TCPConfig,
	infoPolingPeriod time.Duration,
	startTimeout time.Duration,
) *RecordReaderConfig {
	return &RecordReaderConfig{
		dc:               dc,
		namespace:        namespace,
		rewind:           rewind,
		currentHostPort:  currentHostPort,
		tcpConfig:        tcpConfig,
		infoPolingPeriod: infoPolingPeriod,
		startTimeout:     startTimeout,
	}
}

// infoCommander interface for an info client.
type infoCommander interface {
	StartXDR(dc, hostPort, namespace, rewind string) error
	StopXDR(dc string) error
	GetStats(dc, namespace string) (asinfo.Stats, error)
	BlockMRTWrites(namespace string) error
	UnBlockMRTWrites(namespace string) error
}

// RecordReader satisfies the pipeline DataReader interface.
// It reads receives records from an Aerospike database through XDR protocol
// and returns them as *models.ASBXToken.
type RecordReader struct {
	ctx context.Context
	// Info client to start and stop XDR, also to get current state.
	infoClient infoCommander
	// Records reader config.
	config *RecordReaderConfig
	// TCP server to serve XDR backup.
	tcpServer *TCPServer
	// Received results will be placed here.
	results chan *models.ASBXToken
	// Time when recovery finished.
	checkpoint int64
	// To check if the reader is running.
	isRunning atomic.Bool
	// To check if mrt is stopped.
	mrtWritesStopped atomic.Bool

	logger *slog.Logger
}

// NewRecordReader creates a new RecordReader for XDR.
func NewRecordReader(
	ctx context.Context,
	infoClient infoCommander,
	config *RecordReaderConfig,
	logger *slog.Logger,
) (*RecordReader, error) {
	tcpSrv := NewTCPServer(config.tcpConfig, logger)

	rr := &RecordReader{
		ctx:        ctx,
		infoClient: infoClient,
		config:     config,
		tcpServer:  tcpSrv,
		logger:     logger,
	}

	return rr, nil
}

// Read reads the next record from the Aerospike database.
func (r *RecordReader) Read() (*models.ASBXToken, error) {
	// Check if the server already started.
	if !r.isRunning.Load() {
		// If not started.
		if err := r.start(); err != nil {
			return nil, fmt.Errorf("failed to start xdr scan: %w", err)
		}

		// Add timeout after start
		select {
		case res, ok := <-r.results:
			if !ok {
				return nil, io.EOF
			}

			return models.NewASBXToken(res.Key, res.Payload), nil
		case <-time.After(r.config.startTimeout):
			r.Close()

			return nil, fmt.Errorf("xdr scan timed out after: %s", r.config.startTimeout)
		}
	}

	res, ok := <-r.results
	if !ok {
		r.logger.Debug("xdr scan finished")
		return nil, io.EOF
	}

	t := models.NewASBXToken(res.Key, res.Payload)

	return t, nil
}

// Close cancels the Aerospike scan used to read records.
func (r *RecordReader) Close() {
	// If not running, do nothing.
	if !r.isRunning.CompareAndSwap(true, false) {
		return
	}

	r.logger.Debug("closing aerospike xdr record reader")

	if err := r.infoClient.StopXDR(r.config.dc); err != nil {
		r.logger.Error("failed to remove xdr config", slog.Any("error", err))
	}

	// If mrt was stopped.
	if r.mrtWritesStopped.Load() {
		if err := r.infoClient.UnBlockMRTWrites(r.config.namespace); err != nil {
			r.logger.Error("failed to unblock mrt writes", slog.Any("error", err))
		}
		// Only after successful unblocking.
		r.mrtWritesStopped.Store(false)
	}

	if err := r.tcpServer.Stop(); err != nil {
		r.logger.Error("failed to stop tcp server", slog.Any("error", err))
	}

	r.logger.Debug("closed aerospike xdr record reader")
}

func (r *RecordReader) start() error {
	// Create XDR config.
	if err := r.infoClient.StartXDR(
		r.config.dc,
		r.config.currentHostPort,
		r.config.namespace,
		r.config.rewind,
	); err != nil {
		return fmt.Errorf("failed to create xdr config: %w", err)
	}

	r.logger.Debug("created xdr config",
		slog.String("dc", r.config.dc),
		slog.String("hostPort", r.config.currentHostPort),
		slog.String("namespace", r.config.namespace),
		slog.String("rewind", r.config.rewind),
	)

	// Start TCP server.
	results, err := r.tcpServer.Start(r.ctx)
	if err != nil {
		return fmt.Errorf("failed to start xdr: %w", err)
	}

	r.isRunning.Store(true)

	r.results = results

	r.logger.Debug("started xdr tcp server")

	go r.serve()

	return nil
}

func (r *RecordReader) serve() {
	ticker := time.NewTicker(r.config.infoPolingPeriod)
	defer ticker.Stop()
	defer r.Close()

	time.Sleep(statsPollingDelay)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			stats, err := r.infoClient.GetStats(r.config.dc, r.config.namespace)
			if err != nil {
				r.logger.Error("failed to get xdr stats", slog.Any("error", err))
				continue // Or brake?
			}

			r.logger.Debug("got stats", slog.Any("stats", stats),
				slog.String("dc", r.config.dc),
				slog.String("namespace", r.config.namespace),
			)

			if stats.RecoveriesPending != 0 {
				// Recovery in progress.
				continue
			}
			// set once.
			if r.checkpoint == 0 {
				r.checkpoint = time.Now().Unix()
				// Stop MRT writes in this checkpoint.
				if err = r.infoClient.BlockMRTWrites(r.config.namespace); err != nil {
					r.logger.Error("failed to block mrt writes", slog.Any("error", err))
					break // Or return?
				}

				r.logger.Debug("mrt blocked", slog.String("namespace", r.config.namespace))
				r.mrtWritesStopped.Store(true)
			}

			// Convert lag from citrus leaf epoch.
			clLag := cltime.NewCLTime(stats.Lag)
			unixLag := clLag.Unix()

			if r.checkpoint-unixLag < 0 || stats.Lag == 0 {
				// Start MRT writes.
				if err = r.infoClient.UnBlockMRTWrites(r.config.namespace); err != nil {
					r.logger.Error("failed to unblock mrt writes", slog.Any("error", err))
				}

				r.logger.Debug("mrt unblocked", slog.String("namespace", r.config.namespace))
				r.mrtWritesStopped.Store(false)
				// Stop.
				r.Close()

				return
			}
		}
	}
}
