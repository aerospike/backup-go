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
	"math/rand"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
)

// resultChanSize is the size of the channel used to send scan results.
const resultChanSize = 1024

// RecordReaderConfig represents the configuration for scanning Aerospike records.
type RecordReaderConfig struct {
	timeBounds      models.TimeBounds
	partitionFilter *a.PartitionFilter
	// If nodes is set we ignore partitionFilter.
	nodes       []*a.Node
	scanPolicy  *a.ScanPolicy
	scanLimiter *semaphore.Weighted
	namespace   string
	setList     []string
	binList     []string
	noTTLOnly   bool

	// pageSize used for paginated scan for saving reading state.
	// If pageSize = 0, we think that we use normal scan.
	pageSize int64

	rpsCollector *metrics.Collector
}

// NewRecordReaderConfig creates a new RecordReaderConfig.
func NewRecordReaderConfig(namespace string,
	setList []string,
	partitionFilter *a.PartitionFilter,
	nodes []*a.Node,
	scanPolicy *a.ScanPolicy,
	binList []string,
	timeBounds models.TimeBounds,
	scanLimiter *semaphore.Weighted,
	noTTLOnly bool,
	pageSize int64,
	rpsCollector *metrics.Collector,
) *RecordReaderConfig {
	return &RecordReaderConfig{
		namespace:       namespace,
		setList:         setList,
		partitionFilter: partitionFilter,
		nodes:           nodes,
		scanPolicy:      scanPolicy,
		binList:         binList,
		timeBounds:      timeBounds,
		scanLimiter:     scanLimiter,
		noTTLOnly:       noTTLOnly,
		pageSize:        pageSize,
		rpsCollector:    rpsCollector,
	}
}

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

// scanProducer is a function that initiates a scan and returns a channel from which the scan results can be read.
type scanProducer func() (<-chan *a.Result, error)

// RecordReader satisfies the pipeline DataReader interface.
// It reads records from an Aerospike database and returns them as
// *models.Token.
type RecordReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	client scanner
	logger *slog.Logger
	config *RecordReaderConfig
	// pageRecordsChan chan is initialized only if pageSize > 0.
	pageRecordsChan chan *pageRecord
	resultChan      chan *a.Result
	errChan         chan error
	scanOnce        sync.Once
}

// NewRecordReader creates a new RecordReader.
func NewRecordReader(
	ctx context.Context,
	client scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
) *RecordReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeRecord)
	ctx, cancel := context.WithCancel(ctx)

	logger.Debug("created new aerospike record reader")

	return &RecordReader{
		ctx:        ctx,
		cancel:     cancel,
		config:     cfg,
		client:     client,
		logger:     logger,
		resultChan: make(chan *a.Result, resultChanSize),
		errChan:    make(chan error, 1),
	}
}

// Read reads the next record from the Aerospike database.
func (r *RecordReader) Read(ctx context.Context) (*models.Token, error) {
	// If pageSize is set, we use paginated read.
	if r.config.pageSize > 0 {
		return r.readPage(ctx)
	}

	return r.read(ctx)
}

func (r *RecordReader) read(ctx context.Context) (*models.Token, error) {
	r.scanOnce.Do(func() {
		// Start scan with the global context.
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
			r.logger.Error("error reading record", "error", res.Err)
			r.cancel()

			return nil, res.Err
		}

		rec := models.Record{
			Record: res.Record,
		}

		recToken := models.NewRecordToken(&rec, 0, nil)

		r.config.rpsCollector.Increment()

		return recToken, nil
	}
}

// Close cancels the Aerospike scan used to read records if it was started.
func (r *RecordReader) Close() {
	r.logger.Debug("closed aerospike record reader")
}

// startPartitionScan initiates a partition scan for each provided set using the given scan policy and partition filter.
func (r *RecordReader) startScan(ctx context.Context) {
	defer close(r.resultChan)

	// Generate the list of all scan tasks.
	producers, err := r.generateProducers()
	if err != nil {
		r.errChan <- err
		return
	}

	r.shuffleProducers(producers)

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
func (r *RecordReader) executeProducer(ctx context.Context, producer scanProducer) error {
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
	resultsChan, err := producer()
	if err != nil {
		return fmt.Errorf("scan producer failed: %w", err)
	}

	// Drain all results from this specific scan.
	// No context checking here because it slows down the scan.
	for res := range resultsChan {
		r.resultChan <- res
	}

	return nil
}

// generateProducers creates a list of scan-producing functions based on the reader's configuration.
func (r *RecordReader) generateProducers() ([]scanProducer, error) {
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)

	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""} // Scan the entire namespace if no sets are specified.
	}

	var producers []scanProducer

	switch {
	case len(r.config.nodes) > 0:
		for _, node := range r.config.nodes {
			for _, set := range setsToScan {
				// Capture loop variables to ensure the lambda uses the correct values.
				capturedNode, capturedSet := node, set
				producer := func() (<-chan *a.Result, error) {
					r.logger.Debug("starting node scan",
						slog.String("set", capturedSet),
						slog.String("node", capturedNode.GetName()))

					recSet, err := r.client.ScanNode(&scanPolicy, capturedNode, r.config.namespace, capturedSet, r.config.binList...)
					if err != nil {
						return nil, err
					}

					return recSet.Results(), nil
				}
				producers = append(producers, producer)
			}
		}
	case r.config.partitionFilter != nil:
		// Partition Scan Mode
		for _, set := range setsToScan {
			capturedSet := set
			producer := func() (<-chan *a.Result, error) {
				// Each scan requires a fresh copy of the partition filter.
				pf := *r.config.partitionFilter
				r.logger.Debug("starting partition scan",
					slog.String("set", capturedSet),
					slog.Int("begin", pf.Begin),
					slog.Int("count", pf.Count))

				recSet, err := r.client.ScanPartitions(&scanPolicy, &pf, r.config.namespace, capturedSet, r.config.binList...)
				if err != nil {
					return nil, err
				}

				return recSet.Results(), nil
			}
			producers = append(producers, producer)
		}
	default:
		return nil, errors.New("invalid scan parameters: either nodes or partitionFilter must be specified")
	}

	return producers, nil
}

// shuffleProducers randomizes the order of the producers slice to avoid overloading a single node.
func (r *RecordReader) shuffleProducers(producers []scanProducer) {
	rand.Shuffle(len(producers), func(i, j int) {
		producers[i], producers[j] = producers[j], producers[i]
	})
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
