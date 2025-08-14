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
	"fmt"
	"io"
	"log/slog"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
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
// The Aerospike go client satisfies this interface.
//
//go:generate mockery --name scanner
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
	recordSets      chan *a.Recordset
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

	setsNum := len(cfg.setList)
	if len(cfg.setList) == 0 {
		setsNum = 1
	}

	recordSetsSize := setsNum
	if len(cfg.nodes) > 0 {
		recordSetsSize = len(cfg.nodes) * setsNum
	}

	logger.Debug("created new aerospike record reader")

	return &RecordReader{
		ctx:        ctx,
		cancel:     cancel,
		config:     cfg,
		client:     client,
		logger:     logger,
		recordSets: make(chan *a.Recordset, recordSetsSize),
		resultChan: make(chan *a.Result, resultChanSize*setsNum),
		errChan:    make(chan error, 10),
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
func (r *RecordReader) startPartitionScan(ctx context.Context, p *a.ScanPolicy, sets []string) error {
	errGroup, ctx := errgroup.WithContext(ctx)

	for _, set := range sets {
		setName := set // Avoiding clouseress.

		errGroup.Go(func() error {
			return r.scanPartitionsSet(ctx, p, setName)
		})
	}

	return errGroup.Wait()
}

func (r *RecordReader) scanPartitionsSet(ctx context.Context, p *a.ScanPolicy, set string) error {
	// Limit scans if limiter is configured.
	if r.config.scanLimiter != nil {
		err := r.config.scanLimiter.Acquire(ctx, 1)
		if err != nil {
			return fmt.Errorf("failed to acquire scan limiter: %w", err)
		}

		r.logger.Debug("acquired scan limiter 1")
	}

	// Each scan requires a copy of the partition filter.
	pf := *r.config.partitionFilter

	// Scan partitions.
	recSet, err := r.client.ScanPartitions(
		p,
		&pf,
		r.config.namespace,
		set,
		r.config.binList...,
	)
	if err != nil {
		return fmt.Errorf("failed to scan partitions: %w", err)
	}

	// Check context to exit properly.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.recordSets <- recSet: // ok
		r.logger.Debug("set prepared to scan", slog.String("set", set))
	}

	return nil
}

// startNodeScan initiates a node-based scan for each specified set using the provided scan policy and nodes.
func (r *RecordReader) startNodeScan(ctx context.Context, p *a.ScanPolicy, sets []string, nodes []*a.Node) error {
	errGroup, ctx := errgroup.WithContext(ctx)

	for i := range sets {
		// Avoiding clouseress.
		ci := i
		// Each node will have its own scan
		for j := range nodes {
			// Avoiding clouseress.
			cj := j

			errGroup.Go(func() error {
				return r.scanPartitionsNode(ctx, nodes[cj], p, sets[ci])
			})
		}
	}

	return errGroup.Wait()
}

func (r *RecordReader) scanPartitionsNode(ctx context.Context, node *a.Node, p *a.ScanPolicy, set string) error {
	// Limit scans if limiter is configured.
	if r.config.scanLimiter != nil {
		err := r.config.scanLimiter.Acquire(ctx, 1)
		if err != nil {
			return fmt.Errorf("failed to acquire scan limiter: %w", err)
		}
	}

	// Scan nodes.
	recSet, err := r.client.ScanNode(
		p,
		node,
		r.config.namespace,
		set,
		r.config.binList...,
	)
	if err != nil {
		return fmt.Errorf("failed to scan nodes: %w", err)
	}

	// Check context to exit properly.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.recordSets <- recSet: // ok
		r.logger.Debug("set prepared to scan on node",
			slog.String("set", set),
			slog.String("node", node.GetName()),
		)
	}

	return nil
}

// startScan starts the scan for the RecordReader.
func (r *RecordReader) startScan(ctx context.Context) {
	// Prepare scan policy.
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)

	// Prepare set list.
	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	// Start processing results.
	go r.processRecordSets(ctx)

	var err error
	// Run scans according to config.
	switch {
	case len(r.config.nodes) > 0:
		err = r.startNodeScan(ctx, &scanPolicy, setsToScan, r.config.nodes)
	case r.config.partitionFilter != nil:
		err = r.startPartitionScan(ctx, &scanPolicy, setsToScan)
	default:
		err = fmt.Errorf("invalid scan parameters")
	}

	if err != nil {
		r.errChan <- err
	}

	// Close channels after scan complete.
	r.logger.Debug("closing record sets")
	close(r.recordSets)
}

// processRecordSets processes the scan results from recordSets and sends them to the resultChan.
func (r *RecordReader) processRecordSets(ctx context.Context) {
	var wg sync.WaitGroup

	// Order of this defers is important.
	defer close(r.resultChan)
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case set, ok := <-r.recordSets:
			if !ok {
				return
			}
			// Add to wait group before starting processing.
			wg.Add(1)

			go func(set *a.Recordset) {
				defer wg.Done()
				r.processRecords(set)
			}(set)
		}
	}
}

// processRecords processing records set.
func (r *RecordReader) processRecords(set *a.Recordset) {
	// Release limiter on any error. or just on exit.
	defer func() {
		if r.config.scanLimiter != nil {
			r.config.scanLimiter.Release(1)
			r.logger.Debug("released scan limiter 1")
		}
	}()

	for res := range set.Results() {
		r.resultChan <- res
	}
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
