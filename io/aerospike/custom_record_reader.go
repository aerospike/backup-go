package aerospike

import (
	"fmt"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

type scanResult struct {
	records []*a.Result
	Filter  models.PartitionFilterSerialized
}

func newScanResult(bufferSize int64) *scanResult {
	return &scanResult{
		records: make([]*a.Result, 0, bufferSize),
	}
}

// CustomRead reads the next record from the Aerospike database.
func (r *RecordReader) CustomRead() (*models.Token, error) {
	if !r.isCustomScanStarted() {
		scan, err := r.startCustomScan()
		if err != nil {
			return nil, fmt.Errorf("failed to start scan: %w", err)
		}

		r.customScanResults = scan
	}

	res, active := <-r.customScanResults.Results()
	if !active {
		r.logger.Debug("scan finished")
		return nil, io.EOF
	}
	if res.Result == nil {
		return nil, io.EOF
	}
	if res.Result.Err != nil {
		r.logger.Error("error reading record", "error", res.Result.Err)
		return nil, res.Result.Err
	}

	rec := models.Record{
		Record: res.Result.Record,
	}

	recToken := models.NewRecordToken(&rec, 0, res.Filter)

	return recToken, nil
}

// startCustomScan starts the scan for the RecordReader only for state save!
func (r *RecordReader) startCustomScan() (*customRecordSets, error) {
	scanPolicy := *r.config.scanPolicy
	scanPolicy.FilterExpression = getScanExpression(r.config.timeBounds, r.config.noTTLOnly)

	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	if r.config.scanLimiter != nil {
		err := r.config.scanLimiter.Acquire(r.ctx, int64(len(setsToScan)))
		if err != nil {
			return nil, err
		}
	}

	scans := make([]*scanResult, 0, len(setsToScan))

	for _, set := range setsToScan {
		switch {
		case r.config.pageSize > 0:
			recSets, err := r.scanPartitions(
				&scanPolicy,
				r.config.partitionFilter,
				set,
			)
			if err != nil {
				return nil, err
			}

			scans = append(scans, recSets...)
		default:
			return nil, fmt.Errorf("invalid scan parameters")
		}
	}

	return newCustomRecordSets(scans, r.logger), nil
}

func (r *RecordReader) scanPartitions(scanPolicy *a.ScanPolicy,
	partitionFilter *a.PartitionFilter,
	set string,
) ([]*scanResult, error) {
	results := make([]*scanResult, 0)
	scanPolicy.MaxRecords = r.config.pageSize

	for {
		curFilter, err := models.NewPartitionFilterSerialized(partitionFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize partition filter: %w", err)
		}

		recSet, aErr := r.client.ScanPartitions(
			scanPolicy,
			partitionFilter,
			r.config.namespace,
			set,
			r.config.binList...,
		)
		if aErr != nil {
			return nil, fmt.Errorf("failed to scan sets: %w", aErr)
		}

		// result contains []*a.Result and serialized filter models.PartitionFilterSerialized
		result := newScanResult(r.config.pageSize)

		var counter int64
		for res := range recSet.Results() {
			counter++
			if res.Err != nil {
				continue
			} else {
				result.records = append(result.records, res)
			}
		}

		if aErr = recSet.Close(); aErr != nil {
			return nil, fmt.Errorf("failed to close record set: %w", aErr)
		}

		result.Filter = curFilter

		results = append(results, result)
		if counter == 0 {
			break
		}
	}

	return results, nil
}
