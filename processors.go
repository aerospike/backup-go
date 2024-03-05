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
	"fmt"
	"math"

	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/models"
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
			if err != nil {
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

// processorTTL is a dataProcessor that sets the TTL of a record based on its VoidTime.
// It is used during restore to set the TTL of records from their backed up VoidTime.
type processorTTL struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow func() cltime.CLTime
}

// newProcessorTTL creates a new TTLProcessor
func newProcessorTTL() *processorTTL {
	return &processorTTL{
		getNow: cltime.Now,
	}
}

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
			// TODO call a callback to handle expired records
			// for now, filter out expired records
			return nil, nil
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
}

// newProcessorVoidTime creates a new VoidTimeProcessor
func newProcessorVoidTime() *processorVoidTime {
	return &processorVoidTime{
		getNow: cltime.Now,
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
