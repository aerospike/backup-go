// Copyright 2024-2024 Aerospike, Inc.
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

package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"

	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// **** Processor Worker ****

// dataProcessor is an interface for processing data
//
//go:generate mockery --name dataProcessor
type dataProcessor[T any] interface {
	Process(T) (T, error)
}

// processorWorker implements the pipeline.Worker interface
// It wraps a DataProcessor and processes data with it
type processorWorker[T any] struct {
	processor dataProcessor[T]
	receive   <-chan T
	send      chan<- T
}

// newProcessorWorker creates a new ProcessorWorker
func newProcessorWorker[T any](processor dataProcessor[T]) *processorWorker[T] {
	return &processorWorker[T]{
		processor: processor,
	}
}

// SetReceiveChan sets the receive channel for the ProcessorWorker
func (w *processorWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan sets the send channel for the ProcessorWorker
func (w *processorWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

// errFilteredOut is returned by a processor when a token
// should be filtered out of the pipeline
var errFilteredOut = errors.New("filtered out")

// Run starts the ProcessorWorker
func (w *processorWorker[T]) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, active := <-w.receive:
			if !active {
				return nil
			}

			processed, err := w.processor.Process(data)
			if errors.Is(err, errFilteredOut) {
				continue
			} else if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case w.send <- processed:
			}
		}
	}
}

// **** TTL Processor ****

// statsSetterExpired is an interface for setting the number of expired records
//
//go:generate mockery --name statsSetterExpired --inpackage --exported=false
type statsSetterExpired interface {
	addRecordsExpired(uint64)
}

// processorTTL is a dataProcessor that sets the TTL of a record based on its VoidTime.
// It is used during restore to set the TTL of records from their backed up VoidTime.
type processorTTL struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow func() cltime.CLTime
	stats  statsSetterExpired
	logger *slog.Logger
}

// newProcessorTTL creates a new TTLProcessor
func newProcessorTTL(stats statsSetterExpired, logger *slog.Logger) *processorTTL {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeTTL).WithGroup("processor")
	logger.Debug("created new TTL processor")

	return &processorTTL{
		getNow: cltime.Now,
		stats:  stats,
		logger: logger,
	}
}

// errExpiredRecord is returned when a record is expired
// by embedding errFilteredOut, the processor worker will filter out the token
// containing the expired record
var errExpiredRecord = fmt.Errorf("%w: record is expired", errFilteredOut)

// Process sets the TTL of a record based on its VoidTime
func (p *processorTTL) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	record := &token.Record
	now := p.getNow()

	switch {
	case record.VoidTime > 0:
		ttl := record.VoidTime - now.Seconds
		if ttl <= 0 {
			// the record is expired
			p.logger.Debug("record is expired", "digest", record.Key.Digest())
			p.stats.addRecordsExpired(1)

			return nil, errExpiredRecord
		}

		if ttl > math.MaxUint32 {
			return nil, fmt.Errorf("calculated TTL %d is too large", ttl)
		}

		record.Expiration = uint32(ttl)
	case record.VoidTime == 0:
		record.Expiration = models.ExpirationNever
	default:
		return nil, fmt.Errorf("invalid void time %d", record.VoidTime)
	}

	return token, nil
}

// **** VoidTime Processor ****

// processorVoidTime is a dataProcessor that sets the VoidTime of a record based on its TTL
// It is used during backup to set the VoidTime of records from their TTL
// The VoidTime is the time at which the record will expire and is usually what is encoded in backups
type processorVoidTime struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow func() cltime.CLTime
	logger *slog.Logger
}

// newProcessorVoidTime creates a new VoidTimeProcessor
func newProcessorVoidTime(logger *slog.Logger) *processorVoidTime {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeVoidTime).WithGroup("processor")
	logger.Debug("created new VoidTime processor")

	return &processorVoidTime{
		getNow: cltime.Now,
		logger: logger,
	}
}

// Process sets the VoidTime of a record based on its TTL
func (p *processorVoidTime) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	record := &token.Record
	now := p.getNow()

	if record.Expiration == models.ExpirationNever {
		record.VoidTime = models.VoidTimeNeverExpire
	} else {
		record.VoidTime = now.Seconds + int64(record.Expiration)
	}

	return token, nil
}
