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
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/models"
)

// TCPConfig contains tcp server config params.
type TCPConfig struct {
	// TCP server address.
	Address string
	// TLS config for secure connection.
	TLSConfig *tls.Config
	// Timeout in milliseconds for read operations.
	ReadTimoutMilliseconds int64
	// Timeout in milliseconds for write operations.
	WriteTimeoutMilliseconds int64
	// Results queue size.
	ResultQueueSize int
	// Ack messages queue size.
	AckQueueSize int
	// Max number of allowed simultaneous connection to server.
	MaxConnections int
}

// NewDefaultTCPConfig returns default TCP Server config.
func NewDefaultTCPConfig() *TCPConfig {
	return &TCPConfig{
		Address:                  ":8080",
		ReadTimoutMilliseconds:   1000,
		WriteTimeoutMilliseconds: 1000,
		ResultQueueSize:          256,
		AckQueueSize:             256,
		MaxConnections:           100,
	}
}

// TCPServer server for serving XDR connections.
type TCPServer struct {
	config *TCPConfig

	// Fields for internal connection serving.
	listener    net.Listener
	connections chan net.Conn
	wg          sync.WaitGroup
	cancel      context.CancelFunc

	// Results will be sent here.
	resultChan chan *models.XDRToken

	logger *slog.Logger
}

// NewTCPServer returns a new tcp server for serving XDR connections.
func NewTCPServer(
	config *TCPConfig,
	logger *slog.Logger,
) *TCPServer {
	return &TCPServer{
		config:      config,
		connections: make(chan net.Conn, config.MaxConnections),
		resultChan:  make(chan *models.XDRToken, config.ResultQueueSize),
		logger:      logger,
	}
}

// Start launch tcp server for XDR.
func (s *TCPServer) Start(ctx context.Context) (chan *models.XDRToken, error) {
	var err error

	// Redefine cancel function, so we can use it on Stop()
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Create listener
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

	// Start worker pool
	for i := 0; i < s.config.MaxConnections; i++ {
		s.wg.Add(1)
		go s.handleConnections(ctx)
	}

	s.logger.Info("server started",
		slog.String("address", s.config.Address),
		slog.Bool("tls", s.config.TLSConfig != nil))

	return s.resultChan, nil
}

// Stop close listener and all communication channels.
func (s *TCPServer) Stop() {
	s.cancel()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Warn("failed to close tcp server listener", slog.Any("error", err))
		}
	}

	s.wg.Wait()

	close(s.connections)
	close(s.resultChan)

	s.logger.Info("server shutdown complete")
}

// acceptConnections serves connections, not more than maxConnections.
// All connections over pool will be rejected.
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
			select {
			case s.connections <- conn:
				s.logger.Debug("accepted new connection",
					slog.String("address", conn.RemoteAddr().String()))
			default:
				s.logger.Info("connection pool is full, rejecting TCP connection",
					slog.String("address", conn.RemoteAddr().String()))

				if err = conn.Close(); err != nil {
					s.logger.Warn("failed to close connection",
						slog.String("address", conn.RemoteAddr().String()))
				}
			}
		}
	}
}

// handleConnections creates connection handlers to serve each connection.
func (s *TCPServer) handleConnections(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case conn := <-s.connections:
			handler := NewConnectionHandler(
				conn,
				s.resultChan,
				s.config.AckQueueSize,
				s.config.ReadTimoutMilliseconds,
				s.config.WriteTimeoutMilliseconds,
				s.logger,
			)
			handler.Start(ctx)
		}
	}
}

// ConnectionHandler manages a single connection and its acknowledgment queue
type ConnectionHandler struct {
	conn net.Conn
	// channel to send results.
	resultChan chan *models.XDRToken
	// Timeouts in nanoseconds.
	readTimoutNano   int64
	writeTimeoutNano int64

	wg sync.WaitGroup
	// Queue to process received messages.
	bodyQueue chan []byte
	// Queue to process ack messages.
	ackQueue chan []byte

	// Cached values.
	ackMsgSuccess []byte
	ackMsgRetry   []byte
	timeNow       int64

	logger *slog.Logger
}

// NewConnectionHandler returns new connection handler.
// For each connection must be created a separate handler.
func NewConnectionHandler(
	conn net.Conn,
	resultChan chan *models.XDRToken,
	ackQueueSize int,
	readTimout int64,
	writeTimeout int64,
	logger *slog.Logger,
) *ConnectionHandler {
	return &ConnectionHandler{
		conn:             conn,
		resultChan:       resultChan,
		readTimoutNano:   readTimout * 1_000_000,
		writeTimeoutNano: writeTimeout * 1_000_000,
		timeNow:          time.Now().UnixNano(),
		bodyQueue:        make(chan []byte, ackQueueSize),
		ackQueue:         make(chan []byte, ackQueueSize),
		ackMsgSuccess:    NewAckMessage(AckOK),
		ackMsgRetry:      NewAckMessage(AckRetry),
		logger:           logger,
	}
}

// Start launch goroutines to serve current connection.
func (h *ConnectionHandler) Start(ctx context.Context) {
	h.wg.Add(4)

	// This function serve h.timeNow field, to save each 100 milliseconds, current time.
	// This time is used to update deadlines on write and read operations, to improve speed.
	go func() {
		defer h.wg.Done()

		ticker := time.NewTicker(100 * time.Millisecond) // Update every 100ms
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				atomic.StoreInt64(&h.timeNow, t.UnixNano())
			}
		}
	}()

	go func() {
		defer h.wg.Done()
		h.handleMessages(ctx)
	}()

	go func() {
		defer h.wg.Done()
		h.processMessage(ctx)
	}()

	go func() {
		defer h.wg.Done()
		h.handleAcknowledgments(ctx)
	}()

	// Clean up when all routines exit.
	go func() {
		h.wg.Wait()
		h.cleanup()
	}()
}

// cleanup closes communication channels for current handler and closes connection itself.
func (h *ConnectionHandler) cleanup() {
	close(h.ackQueue)
	close(h.bodyQueue)

	if err := h.conn.Close(); err != nil {
		h.logger.Warn("failed to close connection", slog.Any("error", err))
	}
}

// handleMessages processes incoming messages
func (h *ConnectionHandler) handleMessages(ctx context.Context) {
	parser := NewParser(h.conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			timeNow := atomic.LoadInt64(&h.timeNow)
			deadline := timeNow + h.readTimoutNano

			if err := h.conn.SetReadDeadline(time.Unix(deadline, 0)); err != nil {
				h.logger.Error("failed to set read deadline", slog.Any("error", err))

				return
			}

			message, err := parser.Read()

			switch {
			case err == nil:
			// ok.
			case errors.Is(err, io.EOF):
				// do nothing, wait for the next message.
				continue
			case os.IsTimeout(errors.Unwrap(err)):
				// If timeout reached and the connection is closed, do nothing.
				return
			default:
				h.logger.Error("failed to read message", slog.Any("error", err))
				return
			}

			// Process message asynchronously
			h.bodyQueue <- message
		}
	}
}

// processMessage serves h.bodyQueue. When a message is received, we try to parse it
// and send it to h.resultChan, also ack messages is created for this message and sent to h.ackQueue.
func (h *ConnectionHandler) processMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-h.bodyQueue:
			// Parse message.
			aMsg, err := ParseAerospikeMessage(message)
			if err != nil {
				h.logger.Error("failed to parse aerospike message", slog.Any("error", err))
				// If we have an error on parsing message, we send an ack message with retry.
				h.ackQueue <- h.ackMsgRetry

				return
			}
			// Create aerospike key.
			key, err := NewAerospikeKey(aMsg.Fields)
			if err != nil {
				h.logger.Error("failed to parse aerospike key", slog.Any("error", err))
				// If we have an error on parsing message, we send an ack message with retry.
				h.ackQueue <- h.ackMsgRetry

				return
			}
			// Prepare payload.
			// Reset xdr bit.
			message = ResetXDRBit(message)
			// Add headers.
			payload := NewPayload(message)
			// Create token XDRToken.
			token := models.NewXDRToken(key, payload)
			// Send XDRToken to results queue.
			h.resultChan <- token

			// Make acknowledgement.
			h.ackQueue <- h.ackMsgSuccess
		}
	}
}

// handleAcknowledgments manages the async sending of acks.
// Receive messages from h.ackQueue and write them to connection.
func (h *ConnectionHandler) handleAcknowledgments(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-h.ackQueue:
			// Process each received ack message.
			if err := h.sendAck(ack); err != nil {
				h.logger.Error("failed to send ack", slog.Any("error", err))
				continue
			}
		}
	}
}

// sendAck updates connection deadline and writes an ack message to connection.
func (h *ConnectionHandler) sendAck(ack []byte) error {
	timeNow := atomic.LoadInt64(&h.timeNow)
	deadline := timeNow + h.writeTimeoutNano

	if err := h.conn.SetWriteDeadline(time.Unix(0, deadline)); err != nil {
		return fmt.Errorf("failed to set ack write deadline: %w", err)
	}

	// Write an ack message to connection.
	if _, err := h.conn.Write(ack); err != nil {
		return fmt.Errorf("error writing ack to connection: %w", err)
	}

	return nil
}
