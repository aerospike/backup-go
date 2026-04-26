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

// paginatedRecordReader reads records from Aerospike in pages and saves current filter.
type paginatedRecordReader struct {
	ctx             context.Context
	cancel          context.CancelFunc
	client          scanner
	logger          *slog.Logger
	config          *RecordReaderConfig
	scanPolicy      *a.ScanPolicy
	setIndex        int
	pf              *a.PartitionFilter // current active filter for the set
	active          *pageScan
	recordsetCloser RecordsetCloser
}

type pageScan struct {
	taskID             uint64
	results            <-chan *a.Result
	close              func() error
	needsThrottleCheck bool
	pfs                models.PartitionFilterSerialized
	count              uint64
}

// Close cancels the reader context and releases any active scan (recordset + scan slot).
func (r *paginatedRecordReader) Close() {
	r.cancel()
	_ = r.closeActiveScan()
}

func newPaginatedRecordReader(
	ctx context.Context,
	scanner scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
	closer RecordsetCloser,
	cancel context.CancelFunc,
) *paginatedRecordReader {
	logger.Debug("created new paginated aerospike record reader", cfg.logAttrs()...)

	scanPolicy := *cfg.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, cfg.timeBounds, cfg.noTTLOnly)
	scanPolicy.MaxRecords = cfg.pageSize

	return &paginatedRecordReader{
		ctx:             ctx,
		cancel:          cancel,
		client:          scanner,
		logger:          logger,
		config:          cfg,
		scanPolicy:      &scanPolicy,
		recordsetCloser: closer,
	}
}

func (r *paginatedRecordReader) Read(ctx context.Context) (*models.Token, error) {
	for {
		if err := r.ensureActiveScan(ctx); err != nil {
			return nil, err
		}

		res, ok, err := r.readResult(ctx)
		if err != nil {
			return nil, err
		}

		// Channel closed — this page is done
		if !ok {
			if err := r.finishActiveScan(); err != nil {
				return nil, err
			}

			continue
		}

		if shouldSkipPaginatedDrainError(res.Err) {
			continue
		}

		token, err := r.handleResult(res)
		if err != nil {
			return nil, err
		}

		if token == nil {
			continue
		}

		return token, nil
	}
}

func (r *paginatedRecordReader) ensureActiveScan(ctx context.Context) error {
	if r.active != nil {
		return nil
	}

	if err := ctx.Err(); err != nil {
		r.cancel()
		return err
	}

	if err := r.ctx.Err(); err != nil {
		return err
	}

	return r.startNextScan()
}

func (r *paginatedRecordReader) startNextScan() error {
	if r.setIndex >= len(r.config.setList) {
		r.logger.Debug("all sets scanned, done")
		return io.EOF
	}

	if r.pf == nil {
		pf := *r.config.partitionFilter
		r.pf = &pf
	}

	if err := r.ctx.Err(); err != nil {
		return err
	}

	if err := acquireScanSlot(r.ctx, r.config.scanLimiter, r.logger); err != nil {
		return fmt.Errorf("failed to acquire scan slot: %w", err)
	}

	pfs, err := models.NewPartitionFilterSerialized(r.pf)
	if err != nil {
		r.config.scanLimiter.Release(1)
		return fmt.Errorf("failed to serialize partition filter: %w", err)
	}

	set := r.config.setList[r.setIndex]

	recordset, aErr := r.client.ScanPartitions(
		r.scanPolicy,
		r.pf,
		r.config.namespace,
		set,
		r.config.binList...,
	)
	if aErr != nil {
		r.config.scanLimiter.Release(1)
		return fmt.Errorf("failed to start scan: %w", aErr.Unwrap())
	}

	r.logger.Debug("partition scan started",
		slog.Uint64("transactionId", recordset.TaskId()),
		slog.String("set", set),
		slog.String("filter", printPartitionFilter(r.pf)),
	)

	closeRecordset := wrapCloser(r.recordsetCloser, recordset)
	releaseSlot := sync.OnceFunc(func() { r.config.scanLimiter.Release(1) })

	r.active = &pageScan{
		taskID:             recordset.TaskId(),
		results:            recordset.Results(),
		needsThrottleCheck: true,
		pfs:                pfs,
		count:              0,
		close: func() error {
			defer releaseSlot()
			return closeRecordset()
		},
	}

	return nil
}

func (r *paginatedRecordReader) readResult(ctx context.Context) (*a.Result, bool, error) {
	active := r.active
	if active == nil {
		return nil, false, fmt.Errorf("active scan has no results channel")
	}

	// Fast path
	select {
	case res, ok := <-active.results:
		return res, ok, nil
	default:
	}

	select {
	case <-ctx.Done():
		r.cancel()
		_ = r.closeActiveScan()

		return nil, false, ctx.Err()
	case <-r.ctx.Done():
		_ = r.closeActiveScan()
		return nil, false, r.ctx.Err()
	case res, ok := <-active.results:
		return res, ok, nil
	}
}

func (r *paginatedRecordReader) handleResult(res *a.Result) (*models.Token, error) {
	active := r.active
	if active == nil {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("no active scan while handling record result")
	}

	if res == nil {
		return nil, fmt.Errorf("nil scan result")
	}

	if active.needsThrottleCheck {
		active.needsThrottleCheck = false

		if r.config.throttler != nil && shouldThrottle(res.Err) {
			r.logger.Debug("DB under pressure, throttling scan", slog.Any("drainErr", res.Err))

			decoded, err := active.pfs.Decode()
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize partition filter: %w", err)
			}
			r.pf = decoded

			_ = r.closeActiveScan()
			r.config.throttler.Wait(r.ctx)

			return nil, nil
		}
	}

	if res.Err != nil {
		r.cancel()
		_ = r.closeActiveScan()

		return nil, fmt.Errorf("failed to read record: %w", res.Err)
	}

	r.active.count++

	if r.config.rpsCollector != nil {
		r.config.rpsCollector.Increment()
	}

	rec := models.Record{Record: res.Record}

	return models.NewRecordToken(&rec, 0, &active.pfs), nil
}

func (r *paginatedRecordReader) finishActiveScan() error {
	var taskID, count uint64

	if active := r.active; active != nil {
		taskID = active.taskID
		count = active.count
	}

	err := r.closeActiveScan()
	if r.config.throttler != nil {
		r.config.throttler.Notify(r.ctx)
	}

	r.logger.Debug("partition scan finished", slog.Uint64("transactionId", taskID))

	if count == 0 {
		r.setIndex++
		r.pf = nil
	}

	return err
}

func (r *paginatedRecordReader) closeActiveScan() error {
	active := r.active
	if active == nil {
		return nil
	}

	r.active = nil

	return active.close()
}

func shouldSkipPaginatedDrainError(err a.Error) bool {
	if err != nil {
		if err.Matches(types.INVALID_NODE_ERROR) {
			return true
		}
	}

	return false
}
