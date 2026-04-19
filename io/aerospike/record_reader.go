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
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// scanner is an interface for scanning Aerospike records.
type scanner interface {
	ScanPartitions(
		scanPolicy *a.ScanPolicy,
		partitionFilter *a.PartitionFilter,
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

// singleRecordReader reads records from Aerospike namespace one set at a time,
// iterating over setList sequentially. Each set triggers one ScanPartitions call.
type singleRecordReader struct {
	ctx             context.Context
	cancel          context.CancelFunc
	client          scanner
	logger          *slog.Logger
	config          *RecordReaderConfig
	scanPolicy      *a.ScanPolicy
	setIndex        int
	active          *singleScan
	recordsetCloser RecordsetCloser
	closeOnce       sync.Once
	mu              sync.Mutex // active + closeActiveScan (Close vs Read)
}

// singleScan holds state for one active ScanPartitions call.
type singleScan struct {
	taskID             uint64
	results            <-chan *a.Result
	close              func() error
	needsThrottleCheck bool // true until we've inspected the first result for throttle errors
}

// RecordsetCloser is an interface for closing Aerospike recordsets.
// It is required because scanner methods return concrete type Recordset that is impossible to mock.
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
	recordsetCloser RecordsetCloser,
) RecordReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeRecord)
	ctx, cancel := context.WithCancel(ctx)

	if cfg.pageSize > 0 {
		return newPaginatedRecordReader(ctx, client, cfg, logger, recordsetCloser, cancel)
	}

	return newSingleRecordReader(ctx, client, cfg, logger, recordsetCloser, cancel)
}

func newSingleRecordReader(
	ctx context.Context,
	client scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
	recordsetCloser RecordsetCloser,
	cancel context.CancelFunc,
) *singleRecordReader {
	logger.Debug("created new Aerospike record reader", cfg.logAttrs()...)

	return &singleRecordReader{
		ctx:             ctx,
		cancel:          cancel,
		config:          cfg,
		client:          client,
		logger:          logger,
		scanPolicy:      newScanPolicy(cfg),
		recordsetCloser: recordsetCloser,
	}
}

// Read returns the next record token, blocking until one is available.
// It advances through setList automatically, returning io.EOF when all sets are exhausted.
func (r *singleRecordReader) Read(ctx context.Context) (*models.Token, error) {
	for {
		if err := r.ensureActiveScan(ctx); err != nil {
			return nil, err
		}

		res, ok, err := r.readResult(ctx)
		if err != nil {
			return nil, err
		}

		// Channel closed — this set is done; advance to the next one.
		if !ok {
			if err := r.finishActiveScan(); err != nil {
				return nil, err
			}

			continue
		}

		token, err := r.handleResult(res)
		if err != nil {
			return nil, err
		}

		// nil token means we were throttled and retried the same set — loop again.
		if token == nil {
			continue
		}

		return token, nil
	}
}

// ensureActiveScan starts a new scan if none is active, respecting context cancellation.
func (r *singleRecordReader) ensureActiveScan(ctx context.Context) error {
	r.mu.Lock()
	hasActive := r.active != nil
	r.mu.Unlock()

	if hasActive {
		return nil
	}

	// Check both the caller's context and our own internal context.
	select {
	case <-ctx.Done():
		r.cancel()
		return ctx.Err()
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
	}

	return r.startNextScan()
}

// readResult blocks on the active scan's result channel, respecting context cancellation.
func (r *singleRecordReader) readResult(ctx context.Context) (*a.Result, bool, error) {
	r.mu.Lock()
	active := r.active

	var results <-chan *a.Result
	if active != nil {
		results = active.results
	}
	r.mu.Unlock()

	if results == nil {
		return nil, false, fmt.Errorf("active scan has no results channel")
	}

	select {
	case <-ctx.Done():
		r.cancel()
		_ = r.closeActiveScan()

		return nil, false, ctx.Err()
	case <-r.ctx.Done():
		_ = r.closeActiveScan()
		return nil, false, r.ctx.Err()
	case res, ok := <-results:
		return res, ok, nil
	}
}

// handleResult processes one scan result. Returns nil token (no error) when the
// scan was throttled and needs to be retried from the same set.
func (r *singleRecordReader) handleResult(res *a.Result) (*models.Token, error) {
	r.mu.Lock()
	scan := r.active

	var doThrottleCheck bool
	if scan != nil && scan.needsThrottleCheck {
		doThrottleCheck = true
		scan.needsThrottleCheck = false
	}
	r.mu.Unlock()

	if scan == nil {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("no active scan while handling record result")
	}

	if res == nil {
		return nil, fmt.Errorf("nil scan result")
	}

	// On the first result only, check whether the DB signaled throttling.
	// If so, close the scan, wait for capacity, and signal the caller to retry.
	if doThrottleCheck {
		if r.config.throttler != nil && shouldThrottle(res.Err) {
			r.logger.Debug("DB under pressure, throttling scan",
				slog.Any("drainErr", res.Err))
			_ = r.closeActiveScan()
			r.config.throttler.Wait(r.ctx)

			return nil, nil // caller will loop and retry the same set
		}
	}

	if res.Err != nil {
		r.cancel()
		_ = r.closeActiveScan()

		return nil, fmt.Errorf("failed to read record: %w", res.Err)
	}

	if r.config.rpsCollector != nil {
		r.config.rpsCollector.Increment()
	}

	return models.NewRecordToken(&models.Record{Record: res.Record}, 0, nil), nil
}

// finishActiveScan closes the current scan, notifies the throttler, and advances setIndex.
func (r *singleRecordReader) finishActiveScan() error {
	r.mu.Lock()

	var taskID uint64
	if r.active != nil {
		taskID = r.active.taskID
	}
	r.mu.Unlock()

	err := r.closeActiveScan()
	if r.config.throttler != nil {
		r.config.throttler.Notify(r.ctx)
	}

	r.logger.Debug("partition scan finished", slog.Uint64("transactionId", taskID))
	r.setIndex++

	return err
}

// Close cancels the reader context and releases any active scan (recordset + scan slot).
// It is safe to call more than once.
func (r *singleRecordReader) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		_ = r.closeActiveScan()
	})
}

func (r *singleRecordReader) startNextScan() error {
	if r.setIndex >= len(r.config.setList) {
		r.logger.Debug("all sets scanned, done")
		return io.EOF
	}

	if r.config.partitionFilter == nil {
		return fmt.Errorf("partition filter is required for scan")
	}

	if err := r.ctx.Err(); err != nil {
		return err
	}

	if err := acquireScanSlot(r.ctx, r.config.scanLimiter, r.logger); err != nil {
		return fmt.Errorf("failed to acquire scan slot: %w", err)
	}

	pf := *r.config.partitionFilter // fresh copy required per scan
	set := r.config.setList[r.setIndex]

	recordset, err := r.client.ScanPartitions(r.scanPolicy, &pf, r.config.namespace, set, r.config.binList...)
	if err != nil {
		r.config.scanLimiter.Release(1)

		return fmt.Errorf("failed to start scan for set %q namespace %q partitions %d-%d: %w",
			set, r.config.namespace, pf.Begin, pf.Count, err)
	}

	r.logger.Debug("partition scan started",
		slog.Uint64("transactionId", recordset.TaskId()),
		slog.String("set", set),
		slog.String("filter", printPartitionFilter(&pf)),
	)

	closeRecordset := wrapCloser(r.recordsetCloser, recordset)
	releaseSlot := sync.OnceFunc(func() { r.config.scanLimiter.Release(1) })

	r.mu.Lock()
	r.active = &singleScan{
		taskID:             recordset.TaskId(),
		results:            recordset.Results(),
		needsThrottleCheck: true,
		close: func() error {
			defer releaseSlot()
			return closeRecordset()
		},
	}
	r.mu.Unlock()

	return nil
}

// closeActiveScan closes the active scan's recordset.
// r.active is set to nil *before* calling close to prevent any re-entrant close attempt.
func (r *singleRecordReader) closeActiveScan() error {
	r.mu.Lock()
	if r.active == nil {
		r.mu.Unlock()
		return nil
	}

	closeFn := r.active.close
	r.active = nil // clear first — close may call back indirectly
	r.mu.Unlock()

	return closeFn()
}

func newScanPolicy(cfg *RecordReaderConfig) *a.ScanPolicy {
	var scanPolicy a.ScanPolicy
	if cfg.scanPolicy != nil {
		scanPolicy = *cfg.scanPolicy
	}
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, cfg.timeBounds, cfg.noTTLOnly)

	return &scanPolicy
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
	// Unexpired records have TTL = -1.
	return a.ExpEq(a.ExpTTL(), a.ExpIntVal(-1))
}

// noMrtSetExpression returns an expression that filters the monitor set records from the scan results.
func noMrtSetExpression() *a.Expression {
	// where set != "<ERO~MRT"
	return a.ExpNotEq(a.ExpSetName(), a.ExpStringVal(models.MonitorRecordsSetName))
}

// wrapCloser wraps RecordsetCloser.Close function to guarantee it is only executed once.
func wrapCloser(closer RecordsetCloser, recordSet *a.Recordset) func() error {
	var (
		once sync.Once // required to avoid race condition on close
		err  error
	)

	return func() error {
		once.Do(func() {
			err = closer.Close(recordSet)
		})

		return err
	}
}
