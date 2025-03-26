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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/util"
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
	// XDR max throughput number.
	maxThroughput int
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
	maxThroughput int,
) *RecordReaderConfig {
	return &RecordReaderConfig{
		dc:               dc,
		namespace:        namespace,
		rewind:           rewind,
		currentHostPort:  currentHostPort,
		tcpConfig:        tcpConfig,
		infoPolingPeriod: infoPolingPeriod,
		startTimeout:     startTimeout,
		maxThroughput:    maxThroughput,
	}
}

// infoCommander interface for an info client.
type infoCommander interface {
	StartXDR(nodeName, dc, hostPort, namespace, rewind string, throughput int) error
	StopXDR(nodeName, dc string) error
	GetStats(nodeName, dc, namespace string) (asinfo.Stats, error)
	BlockMRTWrites(nodeName, namespace string) error
	UnBlockMRTWrites(nodeName, namespace string) error
	GetNodesNames() []string
}

// RecordReader satisfies the pipeline DataReader interface.
// It reads receives records from an Aerospike database through XDR protocol
// and returns them as *models.ASBXToken.
type RecordReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	// Info client to start and stop XDR, also to get current state.
	infoClient infoCommander
	// Records reader config.
	config *RecordReaderConfig
	// TCP server to serve XDR backup.
	tcpServer *TCPServer
	// Received results will be placed here.
	results chan *models.ASBXToken
	// To check if the reader is running.
	isRunning atomic.Bool

	errorsCh chan error

	nodesRecovered chan struct{}
	activeNodes    []*NodeReader

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
	ctx, cancel := context.WithCancel(ctx)

	rr := &RecordReader{
		ctx:            ctx,
		cancel:         cancel,
		infoClient:     infoClient,
		config:         config,
		tcpServer:      tcpSrv,
		errorsCh:       make(chan error, defaultQueueSize),
		nodesRecovered: make(chan struct{}),
		logger:         logger,
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
			r.logger.Debug("timed out xdr read")
			r.Close()

			return nil, fmt.Errorf("xdr scan timed out after: %s", r.config.startTimeout)
		}
	}

	select {
	case res, ok := <-r.results:
		if !ok {
			r.logger.Debug("xdr scan finished")
			return nil, io.EOF
		}

		t := models.NewASBXToken(res.Key, res.Payload)

		return t, nil
	case err := <-r.errorsCh:
		return nil, err
	}
}

// Close cancels the Aerospike scan used to read records.
func (r *RecordReader) Close() {
	// If not running, do nothing.
	if !r.isRunning.CompareAndSwap(true, false) {
		return
	}

	r.logger.Debug("closing aerospike xdr record reader")

	if err := r.tcpServer.Stop(); err != nil {
		r.logger.Error("failed to stop tcp server", slog.Any("error", err))
	}

	r.logger.Debug("closed aerospike xdr record reader")
}

func (r *RecordReader) start() error {
	// Run TCP server.
	results, err := r.tcpServer.Start(r.ctx)
	if err != nil {
		return fmt.Errorf("failed to start xdr: %w", err)
	}

	go r.serve()

	r.isRunning.Store(true)

	r.results = results

	r.logger.Debug("started xdr tcp server")

	return nil
}

func (r *RecordReader) serve() {
	nodes := r.infoClient.GetNodesNames()

	if len(nodes) == 0 {
		r.logger.Error("no nodes found")
		r.Close()

		return
	}

	var wg sync.WaitGroup

	r.activeNodes = make([]*NodeReader, 0, len(nodes))

	r.createNodeReaders(nodes, &wg)
	go r.watchCluster(nodes, &wg)
	go r.watchNodes()

	wg.Wait()

	r.Close()
}

// createNodeReaders creates node readers for nodes list.
func (r *RecordReader) createNodeReaders(nodes []string, wg *sync.WaitGroup) {
	for _, node := range nodes {
		wg.Add(1)

		n := node
		nr := NewNodeReader(
			r.ctx,
			n,
			r.infoClient,
			r.config,
			r.nodesRecovered,
			r.logger,
		)

		r.activeNodes = append(r.activeNodes, nr)

		go func() {
			defer wg.Done()

			if err := nr.Run(); err != nil {
				r.errorsCh <- fmt.Errorf("failed to start node reader for node %s: %w", node, err)

				// If one of the routine failed, we shut other.
				r.cancel()

				return
			}
		}()
	}
}

// watchNodes monitor nodes state and block mrt when all nodes exit recovery mode.
func (r *RecordReader) watchNodes() {
	var nodesCounter int

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.nodesRecovered:
			nodesCounter++

			if nodesCounter == len(r.activeNodes) {
				// Block mrts on all nodes.
				for _, node := range r.activeNodes {
					err := node.BlockMrt()
					if err != nil {
						r.logger.Error("failed to block mrt for node",
							slog.String("node", node.nodeName),
							slog.Any("error", err))
					}
				}

				r.logger.Debug("all mrt blocked")

				return
			}
		}
	}
}

// watchCluster monitor cluster, and if new node was added, we start new node reader for it.
func (r *RecordReader) watchCluster(nodes []string, wg *sync.WaitGroup) {
	ticker := time.NewTicker(r.config.infoPolingPeriod)
	defer ticker.Stop()

	time.Sleep(statsPollingDelay)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			curNodes := r.infoClient.GetNodesNames()
			if slices.Equal(nodes, curNodes) {
				continue
			}

			diff := util.Diff(nodes, curNodes)
			if len(diff) > 0 {
				r.createNodeReaders(diff, wg)
			}
		}
	}
}
