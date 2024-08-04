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
	"fmt"
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// RecordReaderConfig represents the configuration for scanning Aerospike records.
type RecordReaderConfig struct {
	timeBounds      models.TimeBounds
	partitionFilter *a.PartitionFilter
	scanPolicy      *a.ScanPolicy
	namespace       string
	setList         []string
	binList         []string
}

// NewRecordReaderConfig creates a new RecordReaderConfig.
func NewRecordReaderConfig(namespace string,
	setList []string,
	partitionFilter *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
	binList []string,
	timeBounds models.TimeBounds) *RecordReaderConfig {
	return &RecordReaderConfig{
		namespace:       namespace,
		setList:         setList,
		partitionFilter: partitionFilter,
		scanPolicy:      scanPolicy,
		binList:         binList,
		timeBounds:      timeBounds,
	}
}

// scanner is an interface for scanning Aerospike records
// the Aerospike go client satisfies this interface
//
//go:generate mockery --name scanner
type scanner interface {
	ScanPartitions(
		scanPolicy *a.ScanPolicy,
		partitionFilter *a.PartitionFilter,
		namespace string,
		setName string,
		binNames ...string) (*a.Recordset, a.Error)
}

// RecordReader satisfies the pipeline DataReader interface.
// It reads records from an Aerospike database and returns them as
// *models.Record.
type RecordReader struct {
	client     scanner
	logger     *slog.Logger
	config     *RecordReaderConfig
	scanResult *recordSets
}

// NewRecordReader creates a new RecordReader.
func NewRecordReader(client scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
) *RecordReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeRecord)
	logger.Debug("created new aerospike record reader")

	return &RecordReader{
		config: cfg,
		client: client,
		logger: logger,
	}
}

// Read reads the next record from the Aerospike database.
func (r *RecordReader) Read() (*models.Token, error) {
	if r.scanResult == nil {
		scan, err := r.startScan()
		if err != nil {
			return nil, fmt.Errorf("failed to start scan: %w", err)
		}

		r.scanResult = scan
	}

	res, active := <-r.scanResult.Results()
	if !active {
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
	recToken := models.NewRecordToken(rec, 0)

	return recToken, nil
}

// Close cancels the Aerospike scan used to read records
// if it was started.
func (r *RecordReader) Close() {
	if r.scanResult != nil {
		r.scanResult.Close()
	}

	r.logger.Debug("closed aerospike record reader")
}

// startScan starts the scan for the RecordReader.
func (r *RecordReader) startScan() (*recordSets, error) {
	scanPolicy := *r.config.scanPolicy

	scanPolicy.FilterExpression = timeBoundExpression(r.config.timeBounds)

	setsToScan := r.config.setList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	scans := make([]*a.Recordset, 0, len(setsToScan))

	for _, set := range setsToScan {
		recSet, err := r.client.ScanPartitions(
			&scanPolicy,
			r.config.partitionFilter,
			r.config.namespace,
			set,
			r.config.binList...,
		)
		if err != nil {
			return nil, err
		}

		scans = append(scans, recSet)
	}

	return newRecordSets(scans, r.logger), nil
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
