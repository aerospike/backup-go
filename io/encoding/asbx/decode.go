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

package asbx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

const maxPayloadSize = 10 * 1024 * 1024

// Decoder contains logic for decoding backup data from the binary .asbx format.
// This is a stateful object that should be created for each file being decoded.
type Decoder struct {
	fileNumber int64
	namespace  string
	reader     io.Reader
}

// NewDecoder creates a new Decoder that reads from the provided io.Reader.
func NewDecoder(r io.Reader, fileNumber int64) (*Decoder, error) {
	d := &Decoder{
		reader: r,
	}

	if err := d.readHeader(); err != nil {
		return nil, err
	}
	// TODO: think how to validate order and where to do it.
	if d.fileNumber != fileNumber {
		return nil, errors.New("file number mismatch")
	}

	return d, nil
}

// readHeader reads and validates the file header, returning an error if the header
// is invalid or if a read error occurs.
func (d *Decoder) readHeader() error {
	head := make([]byte, 40)
	if _, err := io.ReadFull(d.reader, head); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("unexpected end of file, header: %w", err)
		}

		return fmt.Errorf("failed to read header: %w", err)
	}

	// Validate version
	if head[0] != version {
		return fmt.Errorf("invalid version: got %d, want %d", head[0], version)
	}

	// Extract file number
	d.fileNumber = int64(binary.BigEndian.Uint32(head[1:5]))

	// Extract namespace (trim trailing zeros)
	d.namespace = string(bytes.TrimRight(head[9:40], "\x00"))
	if d.namespace == "" {
		return fmt.Errorf("namespace is empty")
	}

	return nil
}

// NextToken reads and decodes the next token from the file.
// It returns the decoded token and any error that occurred.
// io.EOF is returned when the end of the file is reached.
func (d *Decoder) NextToken() (*models.XDRToken, error) {
	// Read digest (20 bytes)
	digest := make([]byte, 20)
	_, err := io.ReadFull(d.reader, digest)

	switch {
	case err == nil:
	// ok
	case errors.Is(err, io.EOF):
		return nil, err
	case errors.Is(err, io.ErrUnexpectedEOF):
		return nil, fmt.Errorf("unexpected end of file, digest")
	default:
		return nil, fmt.Errorf("failed to read digest: %w", err)
	}

	// Read payload size (4 bytes)
	sizeBuf := make([]byte, 4)
	if _, err = io.ReadFull(d.reader, sizeBuf); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("unexpected end of file, payload size")
		}

		return nil, fmt.Errorf("failed to read payload size: %w", err)
	}

	payloadSize := binary.BigEndian.Uint32(sizeBuf)
	// 10MB sanity check
	if payloadSize > maxPayloadSize {
		return nil, fmt.Errorf("max payload size reached: %d bytes", payloadSize)
	}

	// Read payload
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(d.reader, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("unexpected end of file, payload")
		}

		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	// TODO: rework this part: set name and key, when client will be ready.
	// Create token
	key, err := aerospike.NewKeyWithDigest(d.namespace, "", "", digest)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	return models.NewXDRToken(key, payload), nil
}
