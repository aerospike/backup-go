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
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
)

// ConnectionHandler manages a single connection and its acknowledgment queue.
type ConnectionHandler struct {
	conn net.Conn
	// channel to send results.
	resultChan chan *models.ASBXToken
	// Timeouts in nanoseconds.
	readTimeoutNano  int64
	writeTimeoutNano int64
	// To stop all goroutines from inside.
	cancel context.CancelFunc
	// Queue to process received messages.
	bodyQueue chan []byte
	// Queue to process ack messages.
	ackQueue chan []byte

	// Cached values.
	ackMsgSuccess []byte
	ackMsgRetry   []byte
	timeNow       int64

	logger           *slog.Logger
	metricsCollector *metrics.Collector
}

// NewConnectionHandler returns a new connection handler.
// A separate handler must be created for each connection.
func NewConnectionHandler(
	conn net.Conn,
	resultChan chan *models.ASBXToken,
	ackQueueSize int,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	logger *slog.Logger,
	metricsCollector *metrics.Collector,
) *ConnectionHandler {
	return &ConnectionHandler{
		conn:             conn,
		resultChan:       resultChan,
		readTimeoutNano:  readTimeout.Nanoseconds(),
		writeTimeoutNano: writeTimeout.Nanoseconds(),
		timeNow:          time.Now().UnixNano(),
		bodyQueue:        make(chan []byte, ackQueueSize),
		ackQueue:         make(chan []byte, ackQueueSize),
		ackMsgSuccess:    NewAckMessage(AckOK),
		ackMsgRetry:      NewAckMessage(AckRetry),
		logger:           logger,
		metricsCollector: metricsCollector,
	}
}

// Start launches goroutines to serve the current connection.
func (h *ConnectionHandler) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	wg := sync.WaitGroup{}
	wg.Add(4)

	// This function serve h.timeNow field, to save each 100 milliseconds, current time.
	// This time is used to update deadlines on write and read operations, to improve speed.
	go func() {
		defer wg.Done()

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
		defer wg.Done()

		h.handleMessages(ctx)
		h.logger.Debug("message handler closed")
	}()

	go func() {
		defer wg.Done()

		h.processMessage(ctx)
		h.logger.Debug("message processor closed")
	}()

	go func() {
		defer wg.Done()

		h.handleAcknowledgments(ctx)
		h.logger.Debug("ack handler closed")
		// After we gracefully closed all channels and stopped goroutines,
		// we can cancel context to stop h.timeNow serving goroutine.
		h.cancel()
	}()

	wg.Wait()
	// When all routines finished we close the connection.
	if err := h.conn.Close(); err != nil {
		h.logger.Warn("failed to close connection", slog.Any("error", err))
	}

	h.logger.Debug("connection closed")
}

// handleMessages processes incoming messages.
func (h *ConnectionHandler) handleMessages(ctx context.Context) {
	parser := NewParser(h.conn)
	// On exit from this function, we close this channel to send signal for the next goroutine to stop.
	defer close(h.bodyQueue)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			deadline := h.getDeadline(h.readTimeoutNano)

			if err := h.conn.SetReadDeadline(time.Unix(0, deadline)); err != nil {
				h.logger.Error("failed to set read deadline", slog.Any("error", err))

				return
			}

			message, err := parser.Read()

			switch {
			case err == nil:
				// ok.
			case errors.Is(err, io.EOF):
				// Exit if there is no message. Aerospike will open new connection if needed.
				return
			case os.IsTimeout(errors.Unwrap(err)):
				// If timeout reached and the connection is closed, do nothing.
				return
			default:
				h.logger.Error("failed to read message", slog.Any("error", err))
				return
			}

			h.metricsCollector.Increment()

			// Process message asynchronously
			h.bodyQueue <- message
		}
	}
}

// processMessage serves h.bodyQueue. When a message is received, we try to parse it
// and send it to h.resultChan, also ack messages is created for this message and sent to h.ackQueue.
func (h *ConnectionHandler) processMessage(ctx context.Context) {
	// On exit from this function, we close this channel to send signal for the next goroutine to stop.
	defer close(h.ackQueue)

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-h.bodyQueue:
			if !ok {
				return
			}
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

			switch {
			case err == nil:
				// ok
			case errors.Is(err, errSkipRecord):
				// Send acknowledgement and skip record.
				h.ackQueue <- h.ackMsgSuccess
				continue
			default:
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
			// Create token ASBXToken.
			token := models.NewASBXToken(key, payload)

			// Message processing take some time,
			// so at the moment we ready to send the result, server can be stopped.
			// Add additional context check before sending token to chan, to avoid sending to closed channel.
			select {
			case <-ctx.Done():
				return
			default:
				// Send ASBXToken to results queue.
				h.resultChan <- token
				// Make acknowledgement.
				h.ackQueue <- h.ackMsgSuccess
			}
		}
	}
}

// handleAcknowledgments manages the async sending of XDR acks.
// It receives messages from h.ackQueue and writes them to the connection socket.
func (h *ConnectionHandler) handleAcknowledgments(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack, ok := <-h.ackQueue:
			if !ok {
				return
			}
			// Process each received ack message.
			if err := h.sendAck(ack); err != nil {
				h.logger.Warn("failed to send ack", slog.Any("error", err))
				// Close connection!
				h.cancel()

				return
			}
		}
	}
}

// sendAck updates connection deadline and writes an ack message to the connection.
func (h *ConnectionHandler) sendAck(ack []byte) error {
	deadline := h.getDeadline(h.writeTimeoutNano)
	if err := h.conn.SetWriteDeadline(time.Unix(0, deadline)); err != nil {
		return fmt.Errorf("failed to set ack write deadline: %w", err)
	}

	// Write an ack message to connection.
	if _, err := h.conn.Write(ack); err != nil {
		return fmt.Errorf("error writing ack to connection: %w", err)
	}

	return nil
}

// getDeadline loads current time from h.timeNow and adds timeout value.
func (h *ConnectionHandler) getDeadline(timeout int64) int64 {
	timeNow := atomic.LoadInt64(&h.timeNow)
	return timeNow + timeout
}
