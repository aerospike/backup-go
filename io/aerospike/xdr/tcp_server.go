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
	"sync"
	"time"

	"github.com/aerospike/backup-go/models"
)

const (
	ackQueueSize    = 256
	resultQueueSize = 256
	maxConnections  = 100
)

// TCPServer server for serving XDR connections.
type TCPServer struct {
	// Config params.
	address      string
	tlsConfig    *tls.Config
	readTimout   time.Duration
	writeTimeout time.Duration

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
	address string,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	tlsConfig *tls.Config,
	logger *slog.Logger,
) *TCPServer {
	return &TCPServer{
		connections:  make(chan net.Conn, maxConnections),
		address:      address,
		readTimout:   readTimeout,
		writeTimeout: writeTimeout,
		tlsConfig:    tlsConfig,
		resultChan:   make(chan *models.XDRToken, resultQueueSize),
		logger:       logger,
	}
}

func (s *TCPServer) Start(ctx context.Context) (chan *models.XDRToken, error) {
	var err error

	// Redefine cancel function, so we can use it on Stop()
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Create listener
	if s.tlsConfig != nil {
		s.listener, err = tls.Listen("tcp", s.address, s.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to start tcp server with tls: %s", err)
		}
	} else {
		s.listener, err = net.Listen("tcp", s.address)
		if err != nil {
			return nil, fmt.Errorf("failed to start tcp server without tls: %s", err)
		}
	}

	// Start connection acceptor
	go s.acceptConnections(ctx)

	// Start worker pool
	for i := 0; i < maxConnections; i++ {
		s.wg.Add(1)
		go s.handleConnections(ctx)
	}

	s.logger.Info("server started",
		slog.String("address", s.address),
		slog.Bool("tls", s.tlsConfig != nil))

	return s.resultChan, nil
}

func (s *TCPServer) Stop() {
	s.cancel()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Warn("failed to close tcp server listener")
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
			handler := NewConnectionHandler(conn, s.resultChan, s.readTimout, s.writeTimeout, s.logger)
			handler.Start(ctx)
		}
	}
}

// ConnectionHandler manages a single connection and its acknowledgment queue
type ConnectionHandler struct {
	conn         net.Conn
	resultChan   chan *models.XDRToken
	readTimout   time.Duration
	writeTimeout time.Duration

	wg       sync.WaitGroup
	ackQueue chan []byte

	logger *slog.Logger
}

func NewConnectionHandler(
	conn net.Conn,
	resultChan chan *models.XDRToken,
	readTimout time.Duration,
	writeTimeout time.Duration,
	logger *slog.Logger,
) *ConnectionHandler {
	return &ConnectionHandler{
		conn:         conn,
		resultChan:   resultChan,
		readTimout:   readTimout,
		writeTimeout: writeTimeout,
		ackQueue:     make(chan []byte, ackQueueSize),
		logger:       logger,
	}
}

// Start begins processing messages and acknowledgments
func (h *ConnectionHandler) Start(ctx context.Context) {
	h.wg.Add(2)

	go func() {
		defer h.wg.Done()
		h.handleMessages(ctx)
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

func (h *ConnectionHandler) cleanup() {
	close(h.ackQueue)
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
			if err := h.conn.SetReadDeadline(time.Now().Add(h.readTimout)); err != nil {
				h.logger.Error("failed to set read deadline", slog.Any("error", err))

				return
			}

			message, err := parser.Read()
			if err != nil {
				if err != io.EOF {
					h.logger.Error("failed to read message", slog.Any("error", err))
				}

				return
			}

			// Process message asynchronously
			go h.processMessage(message)
		}
	}
}

func (h *ConnectionHandler) processMessage(message []byte) {
	// Parse message.
	aMsg, err := ParseAerospikeMessage(message)
	if err != nil {
		h.logger.Error("failed to parse aerospike message", slog.Any("error", err))
		h.ackQueue <- NewAckMessage(AckRetry)

		return
	}
	// Create aerospike key.
	key, err := NewAerospikeKey(aMsg.Fields)
	if err != nil {
		h.logger.Error("failed to parse aerospike key", slog.Any("error", err))
		h.ackQueue <- NewAckMessage(AckRetry)

		return
	}
	// Prepare payload.
	message = ResetXDRBit(message)
	payload := NewPayload(message)
	token := models.NewXDRToken(key, payload)
	// Send payload to results.
	h.resultChan <- token

	// Make acknowledgement.
	h.ackQueue <- NewAckMessage(AckOK)
}

// handleAcknowledgments manages the async sending of acks
func (h *ConnectionHandler) handleAcknowledgments(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-h.ackQueue:
			if err := h.sendAck(ack); err != nil {
				h.logger.Error("failed to send ack", slog.Any("error", err))
				continue
			}
		}
	}
}

func (h *ConnectionHandler) sendAck(ack []byte) error {
	if err := h.conn.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil {
		return fmt.Errorf("failed to set ack write deadline: %s", err)
	}

	// Write an ack message to connection.
	if _, err := h.conn.Write(ack); err != nil {
		return fmt.Errorf("error writing ack to connection: %s", err)
	}

	return nil
}
