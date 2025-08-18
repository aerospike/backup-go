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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// resultChanSize is the size of the channel used to send scan results.
const resultChanSize = 1024

// scanner is an interface for scanning Aerospike records.
type scanner interface {
	ScanPartitions(
		scanPolicy *a.ScanPolicy,
		partitionFilter *a.PartitionFilter,
		namespace string,
		setName string,
		binNames ...string,
	) (*a.Recordset, a.Error)
	ScanNode(
		scanPolicy *a.ScanPolicy,
		node *a.Node,
		namespace string,
		setName string,
		binNames ...string,
	) (*a.Recordset, a.Error)
}

// RecordReader is an interface for reading Aerospike records.
// implements pipe.Reader for a token.
type RecordReader interface {
	Read(ctx context.Context) (*models.Token, error)
	Close()
}

// scanProducer is a function that initiates a scan and returns a channel from which the scan results can be read.
type scanProducer func() (*a.Recordset, error)

// singleRecordReader is a RecordReader that reads records from Aerospike one by one in single thread.
type singleRecordReader struct {
	ctx             context.Context
	cancel          context.CancelFunc
	client          scanner
	logger          *slog.Logger
	config          *RecordReaderConfig
	resultChan      chan *a.Result
	errChan         chan error
	scanOnce        sync.Once
	recordsetCloser RecordsetCloser
}

// RecordsetCloser is an interface for closing Aerospike recordsets.
// It is required because scanner interacted return concrete type Recordset that is impossible to mock.
type RecordsetCloser interface {
	Close(recordset *a.Recordset) a.Error
}

func NewRecordsetCloser() RecordsetCloser {
	return &RecordsetCloserImpl{}
}

type RecordsetCloserImpl struct {
}

func (r *RecordsetCloserImpl) Close(recordset *a.Recordset) a.Error {
	return recordset.Close()
}

// NewRecordReader creates a new RecordReader.
// If the reader is configured to use page size, it will return a paginatedRecordReader,
// it enables saving scan position for resuming.
// Otherwise, it will return a singleRecordReader, that does regular scan in single thread.
func NewRecordReader(
	ctx context.Context,
	client scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
	recodsetCloser RecordsetCloser,
) RecordReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeRecord)
	ctx, cancel := context.WithCancel(ctx)

	if cfg.pageSize > 0 {
		return newPaginatedRecordReader(ctx, client, cfg, logger, recodsetCloser, cancel)
	}

	return newSingleRecordReader(ctx, client, cfg, logger, recodsetCloser, cancel)
}

func newSingleRecordReader(
	ctx context.Context,
	client scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
	recodsetCloser RecordsetCloser,
	cancel context.CancelFunc,
) *singleRecordReader {
	logger.Debug("created new aerospike record reader", cfg.logAttrs()...)

	return &singleRecordReader{
		ctx:             ctx,
		cancel:          cancel,
		config:          cfg,
		client:          client,
		logger:          logger,
		resultChan:      make(chan *a.Result, resultChanSize),
		errChan:         make(chan error, 1),
		recordsetCloser: recodsetCloser,
	}
}

func (r *singleRecordReader) Read(ctx context.Context) (*models.Token, error) {
	r.scanOnce.Do(func() {
		// Start scan with the global context.
		r.logger.Debug("scan started")
		go r.startScan(r.ctx)
	})

	select {
	case <-ctx.Done():
		// If the local context is canceled, we cancel the global context.
		r.cancel()

		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case err := <-r.errChan:
		// serve errors.
		r.cancel()
		return nil, err
	case res, ok := <-r.resultChan:
		if !ok {
			r.logger.Debug("scan finished")
			return nil, io.EOF
		}

		if res.Err != nil {
			r.cancel()
			return nil, fmt.Errorf("error reading record: %w", res.Err)
		}

		rec := models.Record{
			Record: res.Record,
		}

		recToken := models.NewRecordToken(&rec, 0, nil)

		r.config.rpsCollector.Increment()

		return recToken, nil
	}
}

// Close no-op operation to satisfy pipe.Reader interface.
func (r *singleRecordReader) Close() {
}

// startPartitionScan initiates a partition scan for each provided set using the given scan policy and partition filter.
func (r *singleRecordReader) startScan(ctx context.Context) {
	defer close(r.resultChan)

	// Generate the list of all scan tasks.
	producers, err := r.generateProducers()
	if err != nil {
		r.errChan <- err
		return
	}

	// Execute the tasks sequentially.
	for _, producer := range producers {
		if err := r.executeProducer(ctx, producer); err != nil {
			r.errChan <- err
			return
		}
	}
}

// executeProducer runs a single scan task. It acquires a semaphore,
// calls the producer function to start the scan, and drains all results
// from the returned channel before releasing the semaphore.
func (r *singleRecordReader) executeProducer(ctx context.Context, producer scanProducer) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if r.config.scanLimiter != nil {
		if err := r.config.scanLimiter.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("failed to acquire scan limiter: %w", err)
		}
		defer r.config.scanLimiter.Release(1) // Semaphore is released in the same function
	}

	// Call the producer function. This starts the actual Aerospike scan
	// and returns a channel for its results.
	recordset, err := producer()
	if err != nil {
		return fmt.Errorf("scan producer failed: %w", err)
	}

	// Drain all results from this specific scan.
	// No context checking here because it slows down the scan.
	for res := range recordset.Results() {
		r.resultChan <- res
	}

	return r.recordsetCloser.Close(recordset)
}

// generateProducers creates a list of scan-producing functions based on the reader's configuration.
func (r *singleRecordReader) generateProducers() ([]scanProducer, error) {
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)

	var producers []scanProducer

	switch {
	case len(r.config.nodes) > 0:
		for _, node := range r.config.nodes {
			for _, set := range r.config.setList {
				// Capture loop variables to ensure the lambda uses the correct values.
				capturedNode, capturedSet := node, set
				producer := func() (*a.Recordset, error) {
					r.logger.Debug("starting node scan",
						slog.String("set", capturedSet),
						slog.String("node", capturedNode.GetName()))

					recordset, err := r.client.ScanNode(
						&scanPolicy, capturedNode, r.config.namespace, capturedSet, r.config.binList...)
					if err != nil {
						return nil, fmt.Errorf("failed to start scan for set %s, namespace %s, node %s: %w",
							capturedSet, r.config.namespace, capturedNode, err)
					}

					return recordset, nil
				}
				producers = append(producers, producer)
			}
		}

	case r.config.partitionFilter != nil:
		// Partition Scan Mode
		for _, set := range r.config.setList {
			capturedSet := set
			producer := func() (*a.Recordset, error) {
				// Each scan requires a fresh copy of the partition filter.
				pf := *r.config.partitionFilter
				r.logger.Debug("starting partition scan",
					slog.String("set", capturedSet),
					slog.Int("begin", pf.Begin),
					slog.Int("count", pf.Count))

				recordset, err := r.client.ScanPartitions(
					&scanPolicy, &pf, r.config.namespace, capturedSet, r.config.binList...)
				if err != nil {
					return nil, fmt.Errorf("failed to start scan for set %s, namespace %s, filter %d-%d: %w",
						capturedSet, r.config.namespace, pf.Begin, pf.Count, err)
				}

				return recordset, nil
			}
			producers = append(producers, producer)
		}
	default:
		return nil, errors.New("invalid scan parameters: either nodes or partitionFilter must be specified")
	}

	return producers, nil
}

func getScanExpression(currentExpression *a.Expression, bounds models.TimeBounds, noTTLOnly bool) *a.Expression {
	expressions := []*a.Expression{noMrtSetExpression()}

	if currentExpression != nil {
		expressions = append(expressions, currentExpression)
	}

	if exp := timeBoundExpression(bounds); exp != nil {
		expressions = append(expressions, exp)
	}

	if exp := noTTLExpression(noTTLOnly); exp != nil {
		expressions = append(expressions, exp)
	}

	switch len(expressions) {
	case 0:
		return nil
	case 1:
		return expressions[0]
	default:
		return a.ExpAnd(expressions...)
	}
}

func timeBoundExpression(bounds models.TimeBounds) *a.Expression {
	if bounds.FromTime == nil && bounds.ToTime == nil {
		return nil
	}

	if bounds.FromTime != nil && bounds.ToTime == nil {
		return a.ExpGreaterEq(a.ExpLastUpdate(), a.ExpIntVal(bounds.FromTime.UnixNano()))
	}

	if bounds.FromTime == nil && bounds.ToTime != nil {
		return a.ExpLess(a.ExpLastUpdate(), a.ExpIntVal(bounds.ToTime.UnixNano()))
	}

	return a.ExpAnd(
		a.ExpGreaterEq(a.ExpLastUpdate(), a.ExpIntVal(bounds.FromTime.UnixNano())),
		a.ExpLess(a.ExpLastUpdate(), a.ExpIntVal(bounds.ToTime.UnixNano())),
	)
}

func noTTLExpression(noTTLOnly bool) *a.Expression {
	if !noTTLOnly {
		return nil
	}
	// Unexpired records has TTL = -1.
	return a.ExpEq(a.ExpTTL(), a.ExpIntVal(-1))
}

// noMrtSetExpression returns an expression that filters the monitor set records from the scan results.
func noMrtSetExpression() *a.Expression {
	// where set != "<ERO~MRT"
	return a.ExpNotEq(a.ExpSetName(), a.ExpStringVal(models.MonitorRecordsSetName))
}
