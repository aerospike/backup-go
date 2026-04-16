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
}

// RecordReader is an interface for reading Aerospike records.
// implements pipe.Reader for a token.
type RecordReader interface {
	Read(ctx context.Context) (*models.Token, error)
	Close()
}

// singleRecordReader is a RecordReader that reads records from Aerospike one by one in single thread.
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
}

type singleScan struct {
	taskID      uint64
	results     <-chan *a.Result
	close       func() error
	firstResult bool
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

func (r *singleRecordReader) Read(ctx context.Context) (*models.Token, error) {
	for {
		if err := r.ensureActiveScan(ctx); err != nil {
			return nil, err
		}

		res, ok, err := r.readResult(ctx)
		if err != nil {
			return nil, err
		}

		if !ok {
			if err := r.finishActiveScan(); err != nil {
				return nil, err
			}

			continue
		}

		token, retry, err := r.handleResult(res)
		if err != nil {
			return nil, err
		}

		if retry {
			continue
		}

		return token, nil
	}
}

func (r *singleRecordReader) checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		// If the local context is canceled, we cancel the global context.
		r.cancel()
		_ = r.closeActiveScan()

		return ctx.Err()
	case <-r.ctx.Done():
		_ = r.closeActiveScan()
		return r.ctx.Err()
	default:
		return nil
	}
}

func (r *singleRecordReader) ensureActiveScan(ctx context.Context) error {
	if r.active != nil {
		return nil
	}

	if err := r.checkContext(ctx); err != nil {
		return err
	}

	return r.startNextScan()
}

func (r *singleRecordReader) readResult(ctx context.Context) (*a.Result, bool, error) {
	select {
	case <-ctx.Done():
		r.cancel()
		_ = r.closeActiveScan()

		return nil, false, ctx.Err()
	case <-r.ctx.Done():
		_ = r.closeActiveScan()
		return nil, false, r.ctx.Err()
	case res, ok := <-r.active.results:
		return res, ok, nil
	}
}

func (r *singleRecordReader) handleResult(res *a.Result) (*models.Token, bool, error) {
	if r.active.firstResult {
		r.active.firstResult = false

		// Check only the first result for throttling and retry the same set if needed.
		if r.config.throttler != nil && shouldThrottle(res.Err) {
			r.logger.Debug("database hasn't got enough resources, waiting for a signal", slog.Any("drainErr", res.Err))
			_ = r.closeActiveScan()
			r.config.throttler.Wait(r.ctx)

			return nil, true, nil
		}
	}

	if res.Err != nil {
		r.cancel()
		_ = r.closeActiveScan()

		return nil, false, fmt.Errorf("failed to read record: %w", res.Err)
	}

	rec := models.Record{
		Record: res.Record,
	}

	recToken := models.NewRecordToken(&rec, 0, nil)

	r.config.rpsCollector.Increment()

	return recToken, false, nil
}

// Close no-op operation to satisfy pipe.Reader interface.
func (r *singleRecordReader) Close() {
}

func (r *singleRecordReader) startNextScan() error {
	if r.setIndex >= len(r.config.setList) {
		r.logger.Debug("scan finished")
		return io.EOF
	}

	if err := r.ctx.Err(); err != nil {
		return err
	}

	// Check scan limiter. It is required to avoid overloading the DB with too many parallel scans.
	if err := acquireScanSlot(r.ctx, r.config.scanLimiter, r.logger); err != nil {
		return fmt.Errorf("failed to acquire scan limiter: %w", err)
	}

	// Each scan requires a fresh copy of the partition filter.
	pf := *r.config.partitionFilter
	set := r.config.setList[r.setIndex]

	recordset, err := r.client.ScanPartitions(r.scanPolicy, &pf, r.config.namespace, set, r.config.binList...)
	if err != nil {
		r.config.scanLimiter.Release(1)

		return fmt.Errorf("failed to start scan for set %s, namespace %s, filter %d-%d: %w",
			set, r.config.namespace, pf.Begin, pf.Count, err)
	}

	r.logger.Debug("partition scan started",
		slog.Uint64("transactionId", recordset.TaskId()),
		slog.String("set", set),
		slog.String("filter", printPartitionFilter(&pf)),
	)

	// Set close recordset function. Using thread-safe wrapper.
	closeRecordset := wrapCloser(r.recordsetCloser, recordset)
	releaseSlot := sync.OnceFunc(func() {
		r.config.scanLimiter.Release(1)
	})

	r.active = &singleScan{
		taskID:      recordset.TaskId(),
		results:     recordset.Results(),
		firstResult: true,
		close: func() error {
			defer releaseSlot()

			return closeRecordset()
		},
	}

	return nil
}

func (r *singleRecordReader) finishActiveScan() error {
	taskID := uint64(0)
	if r.active != nil {
		taskID = r.active.taskID
	}

	closeErr := r.closeActiveScan()
	r.config.throttler.Notify(r.ctx)
	r.logger.Debug("partition scan finished", slog.Uint64("transactionId", taskID))
	r.setIndex++

	return closeErr
}

func (r *singleRecordReader) closeActiveScan() error {
	if r.active == nil {
		return nil
	}

	closeFn := r.active.close
	r.active = nil

	return closeFn()
}

func newScanPolicy(cfg *RecordReaderConfig) *a.ScanPolicy {
	scanPolicy := *cfg.scanPolicy
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

// monitorContext checks context on a separate goroutine to avoid slowing down record reads.
// When context is canceled, we drain all results from resultChan to avoid deadlock.
func monitorContext[T any](
	ctx context.Context,
	logger *slog.Logger,
	resultChan chan T,
	done chan struct{},
	closeRecordset func() error,
) {
	go func() {
		select {
		case <-ctx.Done():
			// Close record set.
			err := closeRecordset()
			if err != nil {
				logger.Error("failed to close recordset", slog.Any("error", err))
			}

			// Drain records to nowhere.
			for {
				select {
				case _, ok := <-resultChan:
					if !ok {
						// Channel closed, safe to exit
						return
					}
				case <-done:
					// If we have already done, exit.
					return
				}
			}
		case <-done:
			// If everything was OK, exit.
			return
		}
	}()
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
