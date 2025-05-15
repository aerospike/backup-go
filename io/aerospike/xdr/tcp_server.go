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
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
)

const (
	defaultAddress        = ":8080"
	defaultTimeout        = 100 * time.Millisecond
	defaultQueueSize      = 256
	defaultMaxConnections = 4096
)

// TCPConfig contains tcp server config params.
type TCPConfig struct {
	// TCP server address.
	Address string
	// TLS config for secure connection.
	TLSConfig *tls.Config
	// Timeout for read operations.
	ReadTimeout time.Duration
	// Timeout for write operations.
	WriteTimeout time.Duration
	// Results queue size.
	ResultQueueSize int
	// Ack messages queue size.
	AckQueueSize int
	// Max number of allowed simultaneous connection to server.
	MaxConnections int

	rpsCollector *metrics.Collector
}

// NewTCPConfig returns new TCP config.
func NewTCPConfig(
	address string,
	tlsConfig *tls.Config,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	resultQueueSize int,
	ackQueueSize int,
	maxConnections int,
	rpsCollector *metrics.Collector,
) *TCPConfig {
	return &TCPConfig{
		Address:         address,
		TLSConfig:       tlsConfig,
		ReadTimeout:     readTimeout,
		WriteTimeout:    writeTimeout,
		ResultQueueSize: resultQueueSize,
		AckQueueSize:    ackQueueSize,
		MaxConnections:  maxConnections,
		rpsCollector:    rpsCollector,
	}
}

// TCPServer server for serving XDR connections.
type TCPServer struct {
	config *TCPConfig

	// Fields for internal connection serving.
	listener          net.Listener
	activeConnections atomic.Int32
	// Wait group to monitor handlers.
	handlersWg sync.WaitGroup
	cancel     context.CancelFunc

	// Results will be sent here.
	resultChan chan *models.ASBXToken
	isActive   atomic.Bool

	logger *slog.Logger
}

// NewTCPServer returns a new tcp server for serving XDR connections.
func NewTCPServer(
	config *TCPConfig,
	logger *slog.Logger,
) *TCPServer {
	return &TCPServer{
		config: config,
		logger: logger,
	}
}

// Start launch tcp server for XDR.
func (s *TCPServer) Start(ctx context.Context) (chan *models.ASBXToken, error) {
	if !s.isActive.CompareAndSwap(false, true) {
		return nil, errors.New("server start already initiated")
	}

	s.resultChan = make(chan *models.ASBXToken, s.config.ResultQueueSize)

	// Redefine cancel function, so we can use it on Stop()
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Create listener
	var err error
	if s.config.TLSConfig != nil {
		s.listener, err = tls.Listen("tcp", s.config.Address, s.config.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to start tcp server with tls: %w", err)
		}
	} else {
		s.listener, err = net.Listen("tcp", s.config.Address)
		if err != nil {
			return nil, fmt.Errorf("failed to start tcp server without tls: %w", err)
		}
	}

	// Start connection acceptor
	go s.acceptConnections(ctx)

	if s.logger.Enabled(ctx, slog.LevelDebug) {
		go s.reportMetrics(ctx)
	}

	s.logger.Info("server started",
		slog.String("address", s.config.Address),
		slog.Bool("tls", s.config.TLSConfig != nil))

	return s.resultChan, nil
}

// Stop closes the listener and all communication channels.
func (s *TCPServer) Stop() error {
	if !s.isActive.CompareAndSwap(true, false) {
		return fmt.Errorf("server is not active")
	}
	// Stop all routines by context cancelling.
	s.cancel()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Warn("failed to close tcp server listener", slog.Any("error", err))
		}
	}
	// Wait all handlers to stop.
	s.handlersWg.Wait()

	close(s.resultChan)

	s.logger.Info("server shutdown complete")

	return nil
}

// GetActiveConnections returns the number of active connections.
func (s *TCPServer) GetActiveConnections() int32 {
	return s.activeConnections.Load()
}

func (s *TCPServer) reportMetrics(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			conNum := s.GetActiveConnections()
			s.logger.Debug("active connections", slog.Int64("number", int64(conNum)))

		case <-ctx.Done():
			s.logger.Warn("connection metrics stopped")
			return
		}
	}
}

// acceptConnections accepts new connections to the TCPServer.
// It will reject new connections if the number of active connections is greater than MaxConnections.
func (s *TCPServer) acceptConnections(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					s.logger.Info("failed to accept connection", slog.Any("error", err))
				}

				continue
			}
			// Check if we have an opportunity to start new connections.
			if s.activeConnections.Load() < int32(s.config.MaxConnections) {
				// Increment connections counter.
				s.activeConnections.Add(1)
				s.handlersWg.Add(1)

				go func() {
					// Create and run handler.
					handler := NewConnectionHandler(
						conn,
						s.resultChan,
						s.config.AckQueueSize,
						s.config.ReadTimeout,
						s.config.WriteTimeout,
						s.logger,
						s.config.rpsCollector,
					)
					// Handlers wait when all goroutines are finished.
					handler.Start(ctx)
					s.logger.Debug("connection finished",
						slog.String("address", conn.RemoteAddr().String()))
					s.activeConnections.Add(-1)
					s.handlersWg.Done()
				}()

				s.logger.Debug("accepted new connection",
					slog.String("address", conn.RemoteAddr().String()))
			} else {
				s.logger.Error("connection pool is full, rejecting TCP connection",
					slog.String("address", conn.RemoteAddr().String()))

				if err = conn.Close(); err != nil {
					s.logger.Warn("failed to close connection",
						slog.String("address", conn.RemoteAddr().String()))
				}
			}
		}
	}
}
