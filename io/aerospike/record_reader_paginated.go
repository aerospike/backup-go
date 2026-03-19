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
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/models"
)

// pageRecord contains records and serialized filter.
type pageRecord struct {
	result *a.Result
	filter *models.PartitionFilterSerialized
}

// paginatedRecordReader reads records from Aerospike in pages and saves current filter.
type paginatedRecordReader struct {
	ctx             context.Context
	cancel          context.CancelFunc
	client          scanner
	logger          *slog.Logger
	config          *RecordReaderConfig
	resultChan      chan *pageRecord
	errChan         chan error
	scanOnce        sync.Once
	recordsetCloser RecordsetCloser
}

// Close no-op operation to satisfy pipe.Reader interface.
func (r *paginatedRecordReader) Close() {
}

// newPaginatedRecordReader creates a new paginatedRecordReader.
func newPaginatedRecordReader(
	ctx context.Context,
	scanner scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
	closer RecordsetCloser,
	cancel context.CancelFunc,
) *paginatedRecordReader {
	logger.Debug("created new paginated aerospike record reader", cfg.logAttrs()...)

	return &paginatedRecordReader{
		ctx:             ctx,
		cancel:          cancel,
		client:          scanner,
		logger:          logger,
		config:          cfg,
		resultChan:      make(chan *pageRecord, resultChanSize),
		errChan:         make(chan error, 1),
		scanOnce:        sync.Once{},
		recordsetCloser: closer,
	}
}

func newPageRecord(result *a.Result, filter *models.PartitionFilterSerialized) *pageRecord {
	return &pageRecord{
		result: result,
		filter: filter,
	}
}

// readPage reads the next record from pageRecord from the Aerospike database.
func (r *paginatedRecordReader) Read(ctx context.Context) (*models.Token, error) {
	r.scanOnce.Do(func() {
		r.logger.Debug("scan started")

		go r.startScan()
	})

	select {
	case <-ctx.Done():
		r.cancel()
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case err := <-r.errChan:
		r.cancel()
		return nil, err
	case res, ok := <-r.resultChan:
		if !ok {
			r.logger.Debug("scan finished")
			return nil, io.EOF
		}

		if res.result == nil {
			return nil, io.EOF
		}

		if res.result.Err != nil {
			r.cancel()
			return nil, fmt.Errorf("failed to read record: %w", res.result.Err)
		}

		rec := models.Record{
			Record: res.result.Record,
		}

		recToken := models.NewRecordToken(&rec, 0, res.filter)

		r.config.rpsCollector.Increment()

		return recToken, nil
	}
}

// startScan starts the scan for the RecordReader only for state save!
func (r *paginatedRecordReader) startScan() {
	defer close(r.resultChan)

	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)
	scanPolicy.MaxRecords = r.config.pageSize

	for _, set := range r.config.setList {
		if err := r.scanSet(set, &scanPolicy); err != nil {
			r.errChan <- err
			return
		}
	}
}

func (r *paginatedRecordReader) scanSet(set string, scanPolicy *a.ScanPolicy) error {
	// Each scan requires a copy of the partition filter.
	pf := *r.config.partitionFilter

	for {
		count, err := r.scanPage(&pf, scanPolicy, set)
		if err != nil {
			return fmt.Errorf("failed to scan set %s namespace %s: %w", set, r.config.namespace, err)
		}

		if count == 0 { // empty pageRecord
			return nil
		}
	}
}

// scanPage performs a paginated scan on a specific set using the provided partition filter and scan policy.
func (r *paginatedRecordReader) scanPage(
	pf *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
	set string,
) (uint64, error) {
	for {
		retry, count, err := r.scanPageOnce(pf, scanPolicy, set)
		if err != nil {
			return 0, err
		}

		if !retry {
			return count, nil
		}
	}
}

// scanPageOnce performs a single scan over a filtered partition of a given set and returns retry status,
// count, and errors.
func (r *paginatedRecordReader) scanPageOnce(
	pf *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
	set string,
) (retry bool, count uint64, err error) {
	if err := r.ctx.Err(); err != nil {
		return false, 0, err
	}

	// Check scan limiter. It is required to avoid overloading the DB with too many parallel scans.
	if err := acquireScanSlot(r.ctx, r.config.scanLimiter, r.logger); err != nil {
		return false, 0, fmt.Errorf("failed to acquire scan limiter: %w", err)
	}

	pfs, err := models.NewPartitionFilterSerialized(pf)
	if err != nil {
		r.config.scanLimiter.Release(1)

		return false, 0, fmt.Errorf("failed to serialize partition filter: %w", err)
	}

	recordset, aErr := r.client.ScanPartitions( // this scan will read r.config.pageSize records.
		scanPolicy,
		pf,
		r.config.namespace,
		set,
		r.config.binList...,
	)
	if aErr != nil {
		r.config.scanLimiter.Release(1)

		return false, 0, fmt.Errorf("failed to start scan: %w", aErr.Unwrap())
	}

	r.logger.Debug("partition scan started",
		slog.Uint64("transactionId", recordset.TaskId()),
		slog.String("set", set),
		slog.String("filter", printPartitionFilter(pf)),
	)

	// Set close recordset function.
	closeRecordset := wrapCloser(r.ctx, r.recordsetCloser, recordset)

	// Drain all results from this specific scan.
	// No context checking here because it slows down the scan.
	count, drainErr := r.drainResults(pfs, recordset, closeRecordset)

	// Do not return an error immediately, because we need to perform some actions first.
	closeErr := closeRecordset()

	// Release the semaphore manually because of for loop.
	r.config.scanLimiter.Release(1)

	// If we broke out because of a connection error on the first record,
	// we loop back to the top to restart the producer.
	if drainErr != nil {
		r.logger.Debug(
			"database hasn't got enough resources, waiting for a signal",
			slog.Any("drainErr", drainErr),
			slog.Any("closeErr", closeErr),
		)
		// Simple logic first, we just sleep for 10 sec and try again.
		r.config.throttler.Wait(r.ctx)

		// Reset the partition filter to the state before the failed scan.
		// We must copy into *pf rather than reassigning the pointer,
		// because scanSet holds the struct that pf points to.
		decoded, err := pfs.Decode()
		if err != nil {
			return false, 0, fmt.Errorf("failed to deserialize partition filter: %w", err)
		}

		*pf = *decoded

		return true, 0, nil
	}

	// Successfully drained all results, notify the throttler.
	r.config.throttler.Notify(r.ctx)

	r.logger.Debug("partition scan finished", slog.Uint64("transactionId", recordset.TaskId()))

	return false, count, closeErr
}

// drainResults drains results, and if operation failed return error
func (r *paginatedRecordReader) drainResults(
	pfs models.PartitionFilterSerialized,
	recordset *a.Recordset,
	closeRecordset func() error,
) (uint64, error) {
	done := make(chan struct{})
	defer close(done)

	monitorContext[*pageRecord](r.ctx, r.logger, r.resultChan, done, closeRecordset)

	// Used to check the first error.
	var (
		isFirst = true
		count   uint64
	)

	// Drain all results from this specific scan.
	// No context checking here because it slows down the scan.
	for res := range recordset.Results() {
		count++

		if res.Err != nil {
			// When reading the last page (containing 0 records),
			// the scan might return a types.INVALID_NODE_ERROR.
			if res.Err.Matches(types.INVALID_NODE_ERROR) {
				continue
			}
			// check if we should throttle.
			if isFirst && shouldThrottle(res.Err) && r.config.throttler != nil {
				r.logger.Debug("throttling with count", slog.Uint64("count", count))

				return 0, res.Err
			}
		}

		isFirst = false

		r.resultChan <- newPageRecord(res, &pfs)
	}

	return count, nil
}
