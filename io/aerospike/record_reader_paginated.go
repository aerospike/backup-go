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
	pageRecordsChan chan *pageRecord
	errChan         chan error
	scanOnce        sync.Once
	recodsetCloser  RecordsetCloser
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
		pageRecordsChan: make(chan *pageRecord),
		errChan:         make(chan error, 1),
		scanOnce:        sync.Once{},
		recodsetCloser:  closer,
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
	case res, ok := <-r.pageRecordsChan:
		if !ok {
			r.logger.Debug("scan finished")
			return nil, io.EOF
		}

		if res.result == nil {
			return nil, io.EOF
		}

		if res.result.Err != nil {
			r.cancel()
			return nil, fmt.Errorf("error reading record: %w", res.result.Err)
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
	defer close(r.pageRecordsChan)

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
	pf := *r.config.partitionFilter // Each scan requires a copy of the partition filter.

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

func (r *paginatedRecordReader) scanPage(
	pf *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
	set string,
) (uint64, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}

		if r.config.scanLimiter != nil {
			if err := r.config.scanLimiter.Acquire(r.ctx, 1); err != nil {
				return 0, fmt.Errorf("failed to acquire scan limiter: %w", err)
			}
		}

		recSet, aErr := r.client.ScanPartitions( // this scan will read r.config.pageSize records.
			scanPolicy,
			pf,
			r.config.namespace,
			set,
			r.config.binList...,
		)
		if aErr != nil {
			return 0, fmt.Errorf("failed to start scan: %w", aErr.Unwrap())
		}

		count, isThrottled, drainErr := r.drainPageResults(pf, recSet)

		// Close the record set. Do not return an error immediately, because we need to perform some actions first.
		closeErr := r.recodsetCloser.Close(recSet)

		// Release the semaphore manually because of for loop.
		if r.config.scanLimiter != nil {
			r.config.scanLimiter.Release(1)
		}

		// If we broke out because of a connection error on the first record,
		// we loop back to the top to restart the producer.
		if isThrottled {
			r.logger.Debug("database hasn't got enough resources, waiting for a signal",
				slog.Any("error", drainErr))
			// Simple logic first, we just sleep for 10 sec and try again.
			r.config.throttler.Wait(r.ctx)

			continue
		}

		// Successfully drained all results, notify the throttler.
		r.config.throttler.Notify(r.ctx)

		return count, closeErr
	}
}

// drainResults drains results, and if operatrion
func (r *paginatedRecordReader) drainPageResults(pf *a.PartitionFilter, recordset *a.Recordset,
) (count uint64, isThrottled bool, err error) {
	curFilter, err := models.NewPartitionFilterSerialized(pf)
	if err != nil {
		return 0, false, fmt.Errorf("failed to serialize partition filter: %w", err)
	}

	// Used to check the first error.
	var isFirst = true

	// to count records on pageRecord.
	for res := range recordset.Results() {
		if isFirst && shouldThrottle(res.Err) && r.config.throttler != nil {
			return 0, true, res.Err
		}

		if res.Err != nil && !res.Err.Matches(types.INVALID_NODE_ERROR) {
			// When reading last page (containing 0 records), the scan might return an types.INVALID_NODE_ERROR error
			if !res.Err.Matches(types.INVALID_NODE_ERROR) {
				return 0, false, fmt.Errorf("error reading paginated record: %w", res.Err)
			}

			continue
		}

		isFirst = false
		count++

		r.pageRecordsChan <- newPageRecord(res, &curFilter)
	}

	return count, false, nil
}
