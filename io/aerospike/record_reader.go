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
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
)

// RecordReaderConfig represents the configuration for scanning Aerospike records.
type RecordReaderConfig struct {
	timeBounds      models.TimeBounds
	partitionFilter *a.PartitionFilter
	// If nodes is set we ignore partitionFilter.
	nodes       []*a.Node
	scanPolicy  *a.ScanPolicy
	scanLimiter *semaphore.Weighted
	namespace   string
	setList     []string
	binList     []string
	noTTLOnly   bool

	// pageSize used for paginated scan for saving reading state.
	// If pageSize = 0, we think that we use normal scan.
	pageSize int64
}

// NewRecordReaderConfig creates a new RecordReaderConfig.
func NewRecordReaderConfig(namespace string,
	setList []string,
	partitionFilter *a.PartitionFilter,
	nodes []*a.Node,
	scanPolicy *a.ScanPolicy,
	binList []string,
	timeBounds models.TimeBounds,
	scanLimiter *semaphore.Weighted,
	noTTLOnly bool,
	pageSize int64,
) *RecordReaderConfig {
	return &RecordReaderConfig{
		namespace:       namespace,
		setList:         setList,
		partitionFilter: partitionFilter,
		nodes:           nodes,
		scanPolicy:      scanPolicy,
		binList:         binList,
		timeBounds:      timeBounds,
		scanLimiter:     scanLimiter,
		noTTLOnly:       noTTLOnly,
		pageSize:        pageSize,
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
		binNames ...string,
	) (*a.Recordset, a.Error)
	ScanNode(
		scanPolicy *a.ScanPolicy,
		node *a.Node,
		namespace string,
		setName string,
		binNames ...string,
	) (*a.Recordset, a.Error)
}

// RecordReader satisfies the pipeline DataReader interface.
// It reads records from an Aerospike database and returns them as
// *models.Token.
type RecordReader struct {
	ctx        context.Context
	client     scanner
	logger     *slog.Logger
	config     *RecordReaderConfig
	scanResult *recordSets // initialized on first Read() call
	// pageRecordsChan chan is initialized only if pageSize > 0.
	pageRecordsChan chan *pageRecord
}

// NewRecordReader creates a new RecordReader.
func NewRecordReader(
	ctx context.Context,
	client scanner,
	cfg *RecordReaderConfig,
	logger *slog.Logger,
) *RecordReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeRecord)
	logger.Debug("created new aerospike record reader")

	return &RecordReader{
		ctx:    ctx,
		config: cfg,
		client: client,
		logger: logger,
	}
}

// Read reads the next record from the Aerospike database.
func (r *RecordReader) Read() (*models.Token, error) {
	// If pageSize is set, we use paginated read.
	if r.config.pageSize > 0 {
		return r.readPage()
	}

	return r.read()
}

func (r *RecordReader) read() (*models.Token, error) {
	if !r.isScanStarted() {
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

	recToken := models.NewRecordToken(&rec, 0, nil)

	return recToken, nil
}

// Close cancels the Aerospike scan used to read records
// if it was started.
func (r *RecordReader) Close() {
	if r.isScanStarted() {
		r.scanResult.Close()

		if r.config.scanLimiter != nil {
			acquired := max(1, len(r.config.setList)) // when setList is empty, weight 1 is acquired.
			r.config.scanLimiter.Release(int64(acquired))
		}
	}

	r.logger.Debug("closed aerospike record reader")
}

// startScan starts the scan for the RecordReader.
func (r *RecordReader) startScan() (*recordSets, error) {
	scanPolicy := *r.config.scanPolicy

	scanPolicy.FilterExpression = getScanExpression(scanPolicy.FilterExpression, r.config.timeBounds, r.config.noTTLOnly)

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

	scans := make([]*a.Recordset, 0, len(setsToScan))

	for _, set := range setsToScan {
		var (
			recSet *a.Recordset
			err    error
		)

		switch {
		case len(r.config.nodes) > 0:
			recSets, err := r.scanNodes(
				&scanPolicy,
				r.config.nodes,
				set,
			)
			if err != nil {
				return nil, err
			}

			scans = append(scans, recSets...)
		case r.config.partitionFilter != nil:
			recSet, err = r.client.ScanPartitions(
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
		default:
			return nil, fmt.Errorf("invalid scan parameters")
		}
	}

	return newRecordSets(scans, r.logger), nil
}

func (r *RecordReader) scanNodes(scanPolicy *a.ScanPolicy,
	nodes []*a.Node,
	set string,
) ([]*a.Recordset, error) {
	sets := make([]*a.Recordset, 0, len(nodes))

	for i := range nodes {
		recSet, err := r.client.ScanNode(
			scanPolicy,
			nodes[i],
			r.config.namespace,
			set,
			r.config.binList...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan nodes: %w", err)
		}

		sets = append(sets, recSet)
	}

	return sets, nil
}

func (r *RecordReader) isScanStarted() bool {
	return r.scanResult != nil
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
	// Unexpired records has TTL = -1.
	return a.ExpEq(a.ExpTTL(), a.ExpIntVal(-1))
}

// noMrtSetExpression returns expression that filters <ERO~MRT sets from scan results.
func noMrtSetExpression() *a.Expression {
	// where set != "<ERO~MRT"
	return a.ExpNotEq(a.ExpSetName(), a.ExpStringVal(models.MonitorRecordsSetName))
}
