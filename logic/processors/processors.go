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

package processors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/logic/logging"
	"github.com/aerospike/backup-go/logic/util"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// **** Processor Worker ****

// DataProcessor is an interface for processing data
//
//go:generate mockery --name DataProcessor
type DataProcessor[T any] interface {
	Process(T) (T, error)
}

type TokenProcessor = DataProcessor[*models.Token]

// ProcessorWorker implements the pipeline.Worker interface
// It wraps a DataProcessor and processes data with it
type ProcessorWorker[T any] struct {
	processor DataProcessor[T]
	receive   <-chan T
	send      chan<- T
}

// NewProcessorWorker creates a new ProcessorWorker
func NewProcessorWorker[T any](processor DataProcessor[T]) *ProcessorWorker[T] {
	return &ProcessorWorker[T]{
		processor: processor,
	}
}

// SetReceiveChan sets the receive channel for the ProcessorWorker
func (w *ProcessorWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan sets the send channel for the ProcessorWorker
func (w *ProcessorWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

// errFilteredOut is returned by a processor when a token
// should be filtered out of the pipeline
var errFilteredOut = errors.New("filtered out")

// Run starts the ProcessorWorker
func (w *ProcessorWorker[T]) Run(ctx context.Context) error {
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
			p.expired.Add(1)

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

// binFilterProcessor will remove bins with names in binsToRemove from every record it receives.
type binFilterProcessor struct {
	binsToRemove map[string]bool
	skipped      *atomic.Uint64
}

// NewProcessorBinFilter creates new binFilterProcessor with given binList.
func NewProcessorBinFilter(binList []string, skipped *atomic.Uint64) TokenProcessor {
	return &binFilterProcessor{
		binsToRemove: util.ListToMap(binList),
		skipped:      skipped,
	}
}

func (b binFilterProcessor) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	// if filter bin list is empty, don't filter anything.
	if len(b.binsToRemove) == 0 {
		return token, nil
	}

	for key := range token.Record.Bins {
		if !b.binsToRemove[key] {
			delete(token.Record.Bins, key)
		}
	}

	if len(token.Record.Bins) == 0 {
		b.skipped.Add(1)
		return nil, errFilteredOut
	}

	return token, nil
}

type recordCounter struct {
	counter *atomic.Uint64
}

func NewRecordCounter(counter *atomic.Uint64) TokenProcessor {
	return &recordCounter{
		counter: counter,
	}
}

func (c recordCounter) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	c.counter.Add(1)

	return token, nil
}

type sizeCounter struct {
	counter *atomic.Uint64
}

func NewSizeCounter(counter *atomic.Uint64) TokenProcessor {
	return &sizeCounter{
		counter: counter,
	}
}

func (c sizeCounter) Process(token *models.Token) (*models.Token, error) {
	c.counter.Add(token.Size)

	return token, nil
}

// setFilterProcessor filter records by set.
type setFilterProcessor struct {
	setsToRestore map[string]bool
	skipped       *atomic.Uint64
}

// NewProcessorSetFilter creates new setFilterProcessor with given setList.
func NewProcessorSetFilter(setList []string, skipped *atomic.Uint64) TokenProcessor {
	return &setFilterProcessor{
		setsToRestore: util.ListToMap(setList),
		skipped:       skipped,
	}
}

// Process filters out records that does not belong to setsToRestore
func (b setFilterProcessor) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	// if filter set list is empty, don't filter anything.
	if len(b.setsToRestore) == 0 {
		return token, nil
	}

	set := token.Record.Key.SetName()
	if b.setsToRestore[set] {
		return token, nil
	}

	b.skipped.Add(1)

	return nil, errFilteredOut
}

// **** VoidTime Processor ****

// processorVoidTime is a DataProcessor that sets the VoidTime of a record based on its TTL
// It is used during backup to set the VoidTime of records from their TTL
// The VoidTime is the time at which the record will expire and is usually what is encoded in backups
type processorVoidTime struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow func() cltime.CLTime
	logger *slog.Logger
}

// NewProcessorVoidTime creates a new VoidTimeProcessor
func NewProcessorVoidTime(logger *slog.Logger) TokenProcessor {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeVoidTime)
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

// tpsLimiter is a type representing a Token Per Second limiter.
// it does not allow processing more than tps amount of tokens per second.
type tpsLimiter[T any] struct {
	limiter *rate.Limiter
	tps     int
}

// NewTPSLimiter Create a new TPS limiter.
// n — allowed  number of tokens per second, n = 0 means no limit.
func NewTPSLimiter[T any](n int) DataProcessor[T] {
	if n == 0 {
		return &noopProcessor[T]{}
	}

	return &tpsLimiter[T]{
		tps:     n,
		limiter: rate.NewLimiter(rate.Limit(n), 1),
	}
}

// Process delays pipeline if it's needed to match desired rate.
func (t *tpsLimiter[T]) Process(token T) (T, error) {
	if t.tps == 0 {
		return token, nil
	}

	if err := t.limiter.Wait(context.Background()); err != nil {
		var zero T
		return zero, err
	}

	return token, nil
}

// noopProcessor is a no-op implementation of a processor.
type noopProcessor[T any] struct{}

// Process just passes the token through for noopProcessor.
func (n *noopProcessor[T]) Process(token T) (T, error) {
	return token, nil
}

// tokenTypeFilterProcessor is used to support no-records, no-indexes and no-udf flags.
type tokenTypeProcessor struct {
	noRecords bool
	noIndexes bool
	noUdf     bool
}

// NewTokenTypeFilterProcessor creates new tokenTypeFilterProcessor
func NewTokenTypeFilterProcessor(noRecords, noIndexes, noUdf bool) TokenProcessor {
	if !noRecords && !noIndexes && !noUdf {
		return &noopProcessor[*models.Token]{}
	}

	return &tokenTypeProcessor{
		noRecords: noRecords,
		noIndexes: noIndexes,
		noUdf:     noUdf,
	}
}

// Process filters tokens by type.
func (b tokenTypeProcessor) Process(token *models.Token) (*models.Token, error) {
	if b.noRecords && token.Type == models.TokenTypeRecord {
		return nil, fmt.Errorf("%w: record is filtered with no-records flag", errFilteredOut)
	}

	if b.noIndexes && token.Type == models.TokenTypeSIndex {
		return nil, fmt.Errorf("%w: index is filtered with no-indexes flag", errFilteredOut)
	}

	if b.noUdf && token.Type == models.TokenTypeUDF {
		return nil, fmt.Errorf("%w: udf is filtered with no-udf flag", errFilteredOut)
	}

	return token, nil
}

// changeNamespaceProcessor is used to restore to another namespace.
type changeNamespaceProcessor struct {
	restoreNamespace *models.RestoreNamespace
}

// NewChangeNamespaceProcessor creates new changeNamespaceProcessor
func NewChangeNamespaceProcessor(namespace *models.RestoreNamespace) TokenProcessor {
	if namespace == nil {
		return &noopProcessor[*models.Token]{}
	}

	return &changeNamespaceProcessor{
		namespace,
	}
}

// Process filters tokens by type.
func (p changeNamespaceProcessor) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	key := token.Record.Key
	if key.Namespace() != *p.restoreNamespace.Source {
		return nil, fmt.Errorf("invalid namespace %s (expected: %s)", key.Namespace(), *p.restoreNamespace.Source)
	}

	newKey, err := a.NewKeyWithDigest(*p.restoreNamespace.Destination, key.SetName(), key.Value(), key.Digest())
	if err != nil {
		return nil, err
	}

	token.Record.Key = newKey

	return token, nil
}
