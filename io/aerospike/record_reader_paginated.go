package aerospike

import (
	"fmt"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
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
func (r *RecordReader) readPage() (*models.Token, error) {
	errChan := make(chan error)
	if r.pageRecordsChan == nil {
		r.pageRecordsChan = make(chan *pageRecord)
		go r.startScanPaginated(errChan)
	}

	select {
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
			r.logger.Error("error reading record", "error", res.result.Err)
			return nil, res.result.Err
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
func (r *RecordReader) startScanPaginated(localErrChan chan error) {
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(r.config.timeBounds, r.config.noTTLOnly)

	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	if r.config.scanLimiter != nil {
		err := r.config.scanLimiter.Acquire(r.ctx, int64(len(setsToScan)))
		if err != nil {
			localErrChan <- err
			return
		}
	}

	for _, set := range setsToScan {
		resultChan, errChan := r.streamPartitionPages(
			&scanPolicy,
			set,
		)

		for {
			select {
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

// streamPartitionPages reads the whole pageRecord and send it to resultChan.
func (r *RecordReader) streamPartitionPages(
	scanPolicy *a.ScanPolicy,
	set string,
) (resultChan chan []*pageRecord, errChan chan error) {
	scanPolicy.MaxRecords = r.config.pageSize
	// resultChan must not be buffered, we send the whole pageRecord to resultChan.
	// So if we make it buffered, we will consume a lot of RAM.
	resultChan = make(chan []*pageRecord)
	errChan = make(chan error)

	go func() {
		// For one iteration, we scan 1 pageRecord.
		for {
			curFilter, err := models.NewPartitionFilterSerialized(r.config.partitionFilter)
			if err != nil {
				errChan <- fmt.Errorf("failed to serialize partition filter: %w", err)
			}

			recSet, aErr := r.client.ScanPartitions(
				scanPolicy,
				r.config.partitionFilter,
				r.config.namespace,
				set,
				r.config.binList...,
			)
			if aErr != nil {
				errChan <- fmt.Errorf("failed to scan sets: %w", aErr.Unwrap())
			}

			// result contains []*a.Result and serialized filter models.PartitionFilterSerialized
			result := make([]*pageRecord, 0, r.config.pageSize)

			// to count records on pageRecord.
			var counter int64
			for res := range recSet.Results() {
				counter++

				if res.Err != nil {
					continue
				} else {
					// Save to pageRecord filter that returns current pageRecord.
					result = append(result, newPageRecord(res, &curFilter))
				}
			}

			if aErr = recSet.Close(); aErr != nil {
				errChan <- fmt.Errorf("failed to close record set: %w", aErr.Unwrap())
			}

			resultChan <- result
			// If there were no records on the pageRecord, we think that it was last pageRecord and exit.
			if counter == 0 {
				close(resultChan)
				close(errChan)

				return
			}
		}
	}()

	return resultChan, errChan
}
