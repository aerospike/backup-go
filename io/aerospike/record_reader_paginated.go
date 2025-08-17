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
	"math/rand"

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
	r.scanOnce.Do(func() {
		// Initialize paginated channel and start scan with the global context.
		r.pageRecordsChan = make(chan *pageRecord, resultChanSize)
		go r.startScanPaginated(r.ctx)
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
	case res, ok := <-r.pageRecordsChan:
		if !ok {
			r.logger.Debug("paginated scan finished")
			return nil, io.EOF
		}

		if res.result == nil {
			return nil, io.EOF
		}

		if res.result.Err != nil {
			r.logger.Error("error reading paginated record", "error", res.result.Err)
			r.cancel()

			return nil, res.result.Err
		}

		rec := models.Record{
			Record: res.result.Record,
		}

		recToken := models.NewRecordToken(&rec, 0, res.filter)

		r.config.rpsCollector.Increment()

		return recToken, nil
	}
}

// startScanPaginated starts the paginated scan for the RecordReader.
func (r *RecordReader) startScanPaginated(ctx context.Context) {
	defer close(r.pageRecordsChan)

	// Generate the list of all paginated scan tasks.
	producers, err := r.generatePaginatedProducers()
	if err != nil {
		r.errChan <- err
		return
	}

	r.shufflePaginatedProducers(producers)

	// Execute the tasks sequentially.
	for _, producer := range producers {
		if err := r.executePaginatedProducer(ctx, producer); err != nil {
			r.errChan <- err
			return
		}
	}
}

// paginatedScanProducer is a function that initiates a paginated scan and returns a channel from which the scan results can be read.
type paginatedScanProducer func() (<-chan *pageRecord, error)

// executePaginatedProducer runs a single paginated scan task.
func (r *RecordReader) executePaginatedProducer(ctx context.Context, producer paginatedScanProducer) error {
	if r.config.scanLimiter != nil {
		if err := r.config.scanLimiter.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("failed to acquire scan limiter: %w", err)
		}
		defer r.config.scanLimiter.Release(1)
	}

	// Call the producer function. This starts the actual Aerospike paginated scan
	// and returns a channel for its results.
	resultsChan, err := producer()
	if err != nil {
		return fmt.Errorf("paginated scan producer failed: %w", err)
	}

	// Drain all results from this specific paginated scan.
	for res := range resultsChan {
		select {
		case r.pageRecordsChan <- res:
			// Result successfully forwarded to the reader.
		case <-ctx.Done():
			// The reader's context was canceled, so we stop processing.
			r.logger.Debug("context cancelled during paginated record processing")
			return ctx.Err()
		}
	}

	return nil
}

// generatePaginatedProducers creates a list of paginated scan-producing functions based on the reader's configuration.
func (r *RecordReader) generatePaginatedProducers() ([]paginatedScanProducer, error) {
	if r.config.partitionFilter == nil {
		return nil, fmt.Errorf("paginated scan requires partitionFilter")
	}

	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)
	scanPolicy.MaxRecords = r.config.pageSize

	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""} // Scan the entire namespace if no sets are specified.
	}

	var producers []paginatedScanProducer

	for _, set := range setsToScan {
		capturedSet := set
		producer := func() (<-chan *pageRecord, error) {
			return r.streamPartitionPages(&scanPolicy, capturedSet)
		}
		producers = append(producers, producer)
	}

	return producers, nil
}

// shufflePaginatedProducers randomizes the order of the paginated producers slice.
func (r *RecordReader) shufflePaginatedProducers(producers []paginatedScanProducer) {
	rand.Shuffle(len(producers), func(i, j int) {
		producers[i], producers[j] = producers[j], producers[i]
	})
}

// streamPartitionPages performs paginated scanning for a single set.
func (r *RecordReader) streamPartitionPages(scanPolicy *a.ScanPolicy, set string) (<-chan *pageRecord, error) {
	resultChan := make(chan *pageRecord, resultChanSize)

	go func() {
		defer close(resultChan)

		// Each scan requires a copy of the partition filter.
		pf := *r.config.partitionFilter

		r.logger.Debug("starting paginated partition scan", "set", set, "begin", pf.Begin, "count", pf.Count)

		// Continue scanning pages until no more records are found.
		for {
			curFilter, err := models.NewPartitionFilterSerialized(&pf)
			if err != nil {
				r.logger.Error("failed to serialize partition filter", "error", err)
				return
			}

			recSet, aErr := r.client.ScanPartitions(
				scanPolicy,
				&pf,
				r.config.namespace,
				set,
				r.config.binList...,
			)
			if aErr != nil {
				r.logger.Error("failed to scan partitions", "error", aErr)
				return
			}

			var recordCount int64

			// Process all records from this page.
			for res := range recSet.Results() {
				recordCount++

				if res.Err != nil {
					// Ignore last page errors.
					if !res.Err.Matches(types.INVALID_NODE_ERROR) {
						r.logger.Error("error reading paginated record", "error", res.Err)
					}

					continue
				}

				// Send the pageRecord with the current filter state.
				pageRec := newPageRecord(res, &curFilter)
				select {
				case resultChan <- pageRec:
				case <-r.ctx.Done():
					recSet.Close()
					return
				}
			}

			if aErr = r.recodsetCloser.Close(recSet); aErr != nil {
				r.logger.Error("failed to close record set", "error", aErr)
				return
			}

			// If there were no records on this page, we've reached the end.
			if recordCount == 0 {
				r.logger.Debug("paginated scan completed", "set", set)
				return
			}
		}
	}()

	return resultChan, nil
}
