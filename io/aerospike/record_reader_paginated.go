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

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/models"
)

// pageRecord contains records and serialized filter.
type pageRecord struct {
	result *a.Result
	filter *models.PartitionFilterSerialized
}

func newPageRecord(result *a.Result, filter *models.PartitionFilterSerialized) *pageRecord {
	return &pageRecord{
		result: result,
		filter: filter,
	}
}

// readPage reads the next record from pageRecord from the Aerospike database.
func (r *RecordReader) readPage(ctx context.Context) (*models.Token, error) {
	errChan := make(chan error, 1)

	if r.pageRecordsChan == nil {
		r.pageRecordsChan = make(chan *pageRecord)
		go r.startScanPaginated(ctx, errChan)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	case res, active := <-r.pageRecordsChan:
		if !active {
			r.logger.Debug("scan finished")
			return nil, io.EOF
		}

		if res.result == nil {
			return nil, io.EOF
		}

		if res.result.Err != nil {
			return nil, fmt.Errorf("error reading record: %w", res.result.Err)
		}

		rec := models.Record{
			Record: res.result.Record,
		}

		recToken := models.NewRecordToken(&rec, 0, res.filter)

		return recToken, nil
	}

	return nil, nil
}

// startScanPaginated starts the scan for the RecordReader only for state save!
func (r *RecordReader) startScanPaginated(ctx context.Context, localErrChan chan error) {
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)

	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	for _, set := range setsToScan {
		if r.config.scanLimiter != nil {
			err := r.config.scanLimiter.Acquire(r.ctx, 1)
			if err != nil {
				localErrChan <- err
				return
			}

			r.logger.Debug("acquired scan limiter")
		}

		resultChan, errChan := r.streamPartitionPages(
			&scanPolicy,
			set,
		)

		for {
			select {
			case <-ctx.Done():
				localErrChan <- ctx.Err()
				return
			case err, ok := <-errChan:
				if !ok {
					break
				}

				if err != nil {
					localErrChan <- err
					return
				}
			case result, ok := <-resultChan:
				if !ok {
					// After we finish all the readings, we close pageRecord chan.
					close(r.pageRecordsChan)
					close(localErrChan)

					return
				}

				for i := range result {
					r.pageRecordsChan <- result[i]
				}
			}
		}
	}
}

// streamPartitionPages reads the whole pageRecord and sends it to the resultChan.
func (r *RecordReader) streamPartitionPages(
	scanPolicy *a.ScanPolicy,
	set string,
) (resultChan chan []*pageRecord, errChan chan error) {
	scanPolicy.MaxRecords = r.config.pageSize
	// resultChan must not be buffered, we send the whole pageRecord to the resultChan.
	// Implementing buffering would result in substantial RAM consumption.
	resultChan = make(chan []*pageRecord)
	errChan = make(chan error)

	// Each scan requires a copy of the partition filter.
	pf := *r.config.partitionFilter

	go func() {
		// For one iteration, we scan 1 pageRecord.
		for {
			curFilter, err := models.NewPartitionFilterSerialized(&pf)
			if err != nil {
				errChan <- fmt.Errorf("failed to serialize partition filter: %w", err)
			}

			recSet, aErr := r.client.ScanPartitions(
				scanPolicy,
				&pf,
				r.config.namespace,
				set,
				r.config.binList...,
			)
			if aErr != nil {
				errChan <- fmt.Errorf("failed to scan sets: %w", aErr.Unwrap())
				return
			}

			// result contains []*a.Result and serialized filter models.PartitionFilterSerialized
			result := make([]*pageRecord, 0, r.config.pageSize)

			// to count records on pageRecord.
			var counter int64
			for res := range recSet.Results() {
				counter++

				if res.Err != nil {
					// Ignore last page errors.
					if !res.Err.Matches(types.INVALID_NODE_ERROR) {
						r.logger.Error("error reading paginated record", slog.Any("error", res.Err))
					}

					continue
				}
				// Save to pageRecord filter that returns current pageRecord.
				result = append(result, newPageRecord(res, &curFilter))
			}

			if aErr = r.recodsetCloser.Close(recSet); aErr != nil {
				errChan <- fmt.Errorf("failed to close record set: %w", aErr.Unwrap())
			}

			resultChan <- result
			// If there were no records on the pageRecord, we think that it was last pageRecord and exit.
			if counter == 0 {
				close(resultChan)
				close(errChan)

				if r.config.scanLimiter != nil {
					r.config.scanLimiter.Release(1)
					r.logger.Debug("scan limiter released")
				}

				return
			}
		}
	}()

	return resultChan, errChan
}
