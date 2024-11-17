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

package aerospike

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/aerospike/backup-go/models"
)

type TCPServer struct {
	address    string
	readTimout time.Duration
	tlsConfig  *tls.Config
	// Results will be sent here.
	resultChan chan *models.XDRToken
	logger     *slog.Logger
}

func NewTCPServer(
	address string,
	readTimeout time.Duration,
	tlsConfig *tls.Config,
	resultChan chan *models.XDRToken,
	logger *slog.Logger,
) *TCPServer {
	return &TCPServer{
		address:    address,
		readTimout: readTimeout,
		tlsConfig:  tlsConfig,
		resultChan: resultChan,
		logger:     logger,
	}
}

func (s *TCPServer) Start(ctx context.Context) error {
	var (
		listener net.Listener
		err      error
	)

	if s.tlsConfig != nil {
		listener, err = tls.Listen("tcp", s.address, s.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to start tcp server with tls: %s", err)
		}

		s.logger.Debug("started tcp server with tls")
	} else {
		listener, err = net.Listen("tcp", s.address)
		if err != nil {
			return fmt.Errorf("failed to start tcp server without tls: %s", err)
		}

		s.logger.Debug("started tcp server without tls")
	}

	defer listener.Close()

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("tcp server context done")
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Error("failed to accept tcp connection", slog.Any("error", err))
				continue
			}

			go s.handleConn(ctx, conn)
		}
	}
}

func (s *TCPServer) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	s.logger.Debug("handling tcp connection", slog.String("client", conn.RemoteAddr().String()))

	for {
		if ctx.Err() != nil {
			s.logger.Error("handle connection context", slog.Any("error", ctx.Err()))
			return
		}

		if err := conn.SetDeadline(time.Now().Add(s.readTimout)); err != nil {
			s.logger.Error("failed to set deadline",
				slog.String("client", conn.RemoteAddr().String()),
				slog.Duration("timeout", s.readTimout),
			)
		}

		// read from connection.
		req, err := s.readRequest(conn)
		if err != nil {
			s.logger.Error("failed to read request", slog.String("client", conn.RemoteAddr().String()))
			continue
		}

		// parse request.
		token := models.NewXDRToken(req)
		s.resultChan <- token
		// write response. If needed.
		s.logger.Debug("parsed request",
			slog.String("client", conn.RemoteAddr().String()),
			slog.Any("token", token),
		)
	}
}

// readRequest example of implementation.
// TODO: Will be replaced after protocol will be ready.
func (s *TCPServer) readRequest(conn net.Conn) ([]byte, error) {
	header, err := readBytes(conn, 4)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)

	s.logger.Debug("received request",
		slog.String("client", conn.RemoteAddr().String()),
		slog.Any("length", length),
	)

	return readBytes(conn, int(length))
}

// readBytes reading `length` number of bytes from `conn`.
func readBytes(conn net.Conn, length int) ([]byte, error) {
	buffer := make([]byte, length)
	total := 0

	for total < length {
		n, err := conn.Read(buffer[total:])
		if err != nil {
			return nil, err
		}

		total += n
	}

	return buffer, nil
}
