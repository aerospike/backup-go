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
	"golang.org/x/sync/semaphore"
)

const resultChanSize = 256

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
	logger.Debug("created new aerospike record reader")

	setsNum := len(cfg.setList)
	if len(cfg.setList) == 0 {
		setsNum = 1
	}

	return &RecordReader{
		ctx:        ctx,
		config:     cfg,
		client:     client,
		logger:     logger,
		recordSets: make(chan *a.Recordset, setsNum),
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
		go r.startScan(ctx)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-r.errChan:
		// serve errors.
		return nil, err
	case res, ok := <-r.resultChan:
		if !ok {
			r.logger.Debug("scan finished")
			return nil, io.EOF
		}

		if res.Err != nil {
			r.logger.Error("error reading record", "error", res.Err)
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
	close(r.errChan)
	// We close everything in another place, so this is no-op close.
	r.logger.Debug("closed aerospike record reader")
}

// startPartitionScan initiates a partition scan for each provided set using the given scan policy and partition filter.
func (r *RecordReader) startPartitionScan(ctx context.Context, p *a.ScanPolicy, sets []string) {
	for _, set := range sets {
		// Limit scans if limiter is configured.
		if r.config.scanLimiter != nil {
			err := r.config.scanLimiter.Acquire(r.ctx, 1)
			if err != nil {
				r.errChan <- fmt.Errorf("failed to acquire scan limiter: %w", err)
				return
			}
		}

		// Scan partitions.
		recSet, err := r.client.ScanPartitions(
			p,
			r.config.partitionFilter,
			r.config.namespace,
			set,
			r.config.binList...,
		)
		if err != nil {
			r.errChan <- fmt.Errorf("failed to scan partitions: %w", err)
			return
		}

		// Check context to exit properly.
		select {
		case <-ctx.Done():
			return
		case r.recordSets <- recSet: // ok
		}
	}
}

// startNodeScan initiates a node-based scan for each specified set using the provided scan policy and nodes.
func (r *RecordReader) startNodeScan(ctx context.Context, p *a.ScanPolicy, sets []string, nodes []*a.Node) {
	for _, set := range sets {
		// Each node will have it own scan
		for i := range nodes {
			// Limit scans if limiter is configured.
			if r.config.scanLimiter != nil {
				err := r.config.scanLimiter.Acquire(r.ctx, 1)
				if err != nil {
					r.errChan <- fmt.Errorf("failed to acquire scan limiter: %w", err)
					return
				}
			}

			// Scan nodes.
			recSet, err := r.client.ScanNode(
				p,
				nodes[i],
				r.config.namespace,
				set,
				r.config.binList...,
			)
			if err != nil {
				r.errChan <- fmt.Errorf("failed to scan nodes: %w", err)
				return
			}

			// Check context to exit properly.
			select {
			case <-ctx.Done():
				return
			case r.recordSets <- recSet: // ok
			}
		}
	}
}

// startScan starts the scan for the RecordReader.
func (r *RecordReader) startScan(ctx context.Context) {
	// Close channels after scan complete.
	defer close(r.recordSets)

	// Prepare scan policy.
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)

	// Prepare set list.
	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	// Start processing results.
	go r.processScanResults(ctx)

	// Run scans according to config.
	switch {
	case len(r.config.nodes) > 0:
		r.startNodeScan(ctx, &scanPolicy, setsToScan, r.config.nodes)
	case r.config.partitionFilter != nil:
		r.startPartitionScan(ctx, &scanPolicy, setsToScan)
	default:
		r.errChan <- fmt.Errorf("invalid scan parameters")
		return
	}
}

// processScanResults processes the scan results from recordSets and sends them to the resultChan.
func (r *RecordReader) processScanResults(ctx context.Context) {
	defer close(r.resultChan)

	// Iterate over all data sets.
	for recordSet := range r.recordSets {
		// Iterate over all records in a set.
		for res := range recordSet.Results() {
			select {
			case <-ctx.Done():
				return
			case r.resultChan <- res:
			}
		}

		if r.config.scanLimiter != nil {
			r.config.scanLimiter.Release(1)
		}
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
