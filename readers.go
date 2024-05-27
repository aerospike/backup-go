// Copyright 2024-2024 Aerospike, Inc.
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

package backup

import (
	"context"
	"errors"
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/logic/logging"
	"github.com/aerospike/backup-go/logic/util"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// **** Read Worker ****

// dataReader is an interface for reading data from a source.
//
//go:generate mockery --name dataReader
type dataReader[T any] interface {
	Read() (T, error)
	Close()
}

// readWorker implements the pipeline.Worker interface
// It wraps a DataReader and reads data from it
type readWorker[T any] struct {
	reader dataReader[T]
	send   chan<- T
}

// newReadWorker creates a new ReadWorker
func newReadWorker[T any](reader dataReader[T]) *readWorker[T] {
	return &readWorker[T]{
		reader: reader,
	}
}

// SetReceiveChan satisfies the pipeline.Worker interface
// but is a no-op for the ReadWorker
func (w *readWorker[T]) SetReceiveChan(_ <-chan T) {
	// no-op
}

// SetSendChan sets the send channel for the ReadWorker
func (w *readWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

// Run runs the ReadWorker
func (w *readWorker[T]) Run(ctx context.Context) error {
	defer w.reader.Close()

	for {
		data, err := w.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case w.send <- data:
		}
	}
}

// **** Token Reader ****

// tokenReader satisfies the DataReader interface
// It reads data as tokens using a Decoder
type tokenReader struct {
	decoder encoding.Decoder
	logger  *slog.Logger
}

// newTokenReader creates a new GenericReader
func newTokenReader(decoder encoding.Decoder, logger *slog.Logger) *tokenReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeToken)
	logger.Debug("created new token reader")

	return &tokenReader{
		decoder: decoder,
		logger:  logger,
	}
}

// Read reads the next token from the decoder
func (dr *tokenReader) Read() (*models.Token, error) {
	return dr.decoder.NextToken()
}

// Close satisfies the DataReader interface
// but is a no-op for the tokenReader
func (dr *tokenReader) Close() {
	dr.logger.Debug("closed token reader")
}

// **** Aerospike DB Reader ****

// arrConfig is the configuration for an AerospikeRecordReader
type arrConfig struct {
	timeBounds     models.TimeBounds
	Namespace      string
	SetList        []string
	BinList        []string
	PartitionRange PartitionRange
}

func newArrConfig(backupConfig *BackupConfig, partitions PartitionRange) *arrConfig {
	return &arrConfig{
		Namespace:      backupConfig.Namespace,
		SetList:        backupConfig.SetList,
		PartitionRange: partitions,
		BinList:        backupConfig.BinList,
		timeBounds: models.TimeBounds{
			FromTime: backupConfig.ModAfter,
			ToTime:   backupConfig.ModBefore,
		},
	}
}

// arrStatus is the status of an AerospikeRecordReader
type arrStatus struct {
	started bool
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

// aerospikeRecordReader satisfies the DataReader interface
// It reads records from an Aerospike database and returns them as *models.Record
type aerospikeRecordReader struct {
	client     scanner
	scanPolicy *a.ScanPolicy
	recResChan <-chan *a.Result
	logger     *slog.Logger
	config     *arrConfig
	recSet     []*a.Recordset
	status     arrStatus
}

// newAerospikeRecordReader creates a new AerospikeRecordReader
func newAerospikeRecordReader(client scanner, cfg *arrConfig,
	scanPolicy *a.ScanPolicy, logger *slog.Logger) *aerospikeRecordReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeRecord)
	logger.Debug("created new aerospike record reader")

	job := &aerospikeRecordReader{
		config:     cfg,
		client:     client,
		scanPolicy: scanPolicy,
		logger:     logger,
	}

	return job
}

// Read reads the next record from the Aerospike database
func (arr *aerospikeRecordReader) Read() (*models.Token, error) {
	if !arr.status.started {
		err := arr.startScan()
		if err != nil {
			return nil, err
		}

		arr.status.started = true
	}

	res, active := <-arr.recResChan
	if !active {
		arr.logger.Debug("scan finished")
		return nil, io.EOF
	}

	if res.Err != nil {
		arr.logger.Error("error reading record", "error", res.Err)
		return nil, res.Err
	}

	rec := models.Record{
		Record: res.Record,
	}
	recToken := models.NewRecordToken(rec, 0)

	return recToken, nil
}

// Close cancels the Aerospike scan used to read records
// if it was started
func (arr *aerospikeRecordReader) Close() {
	arr.status.started = false
	if arr.recSet != nil {
		// ignore this error, it only happens if the scan is already closed
		// and this method can not return an error anyway
		for _, rec := range arr.recSet {
			if err := rec.Close(); err != nil {
				arr.logger.Error("error while closing record set", "error", rec.Close())
			}
		}
	}

	arr.logger.Debug("closed aerospike record reader")
}

// startScan starts the scan for aerospikeRecordReader
func (arr *aerospikeRecordReader) startScan() error {
	arr.recResChan = make(chan *a.Result)

	partitionFilter := a.NewPartitionFilterByRange(
		arr.config.PartitionRange.Begin,
		arr.config.PartitionRange.Count,
	)

	arr.scanPolicy.FilterExpression = timeBoundExpression(arr.config.timeBounds)

	setsToScan := arr.config.SetList
	if len(setsToScan) == 0 {
		setsToScan = []string{""}
	}

	resultChannels := make([]<-chan *a.Result, len(arr.recSet))

	for _, set := range setsToScan {
		recSet, err := arr.client.ScanPartitions(
			arr.scanPolicy,
			partitionFilter,
			arr.config.Namespace,
			set,
			arr.config.BinList...,
		)
		if err != nil {
			return err
		}

		arr.recSet = append(arr.recSet, recSet)
		resultChannels = append(resultChannels, recSet.Results())
	}

	arr.recResChan = util.MergeChannels(resultChannels)

	return nil
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

// **** Aerospike SIndex Reader ****

// sindexGetter is an interface for getting secondary indexes
//
//go:generate mockery --name sindexGetter
type sindexGetter interface {
	GetSIndexes(namespace string) ([]*models.SIndex, error)
}

// sindexReader satisfies the DataReader interface
// It reads secondary indexes from a SIndexGetter and returns them as *models.SecondaryIndex
type sindexReader struct {
	client    sindexGetter
	sindexes  chan *models.SIndex
	logger    *slog.Logger
	namespace string
}

// newSIndexReader creates a new SIndexReader
func newSIndexReader(client sindexGetter, namespace string, logger *slog.Logger) *sindexReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeSIndex)
	logger.Debug("created new sindex reader")

	return &sindexReader{
		client:    client,
		namespace: namespace,
		logger:    logger,
	}
}

// Read reads the next secondary index from the SIndexGetter
func (r *sindexReader) Read() (*models.Token, error) {
	// grab all the sindexes on the first run
	if r.sindexes == nil {
		r.logger.Debug("fetching all secondary indexes")

		sindexes, err := r.client.GetSIndexes(r.namespace)
		if err != nil {
			return nil, err
		}

		r.sindexes = make(chan *models.SIndex, len(sindexes))
		for _, sindex := range sindexes {
			r.sindexes <- sindex
		}
	}

	if len(r.sindexes) > 0 {
		SIToken := models.NewSIndexToken(<-r.sindexes, 0)
		return SIToken, nil
	}

	return nil, io.EOF
}

// Close satisfies the DataReader interface
// but is a no-op for the SIndexReader
func (r *sindexReader) Close() {}

// **** Aerospike UDF Reader ****

// udfGetter is an interface for getting UDFs
//
//go:generate mockery --name udfGetter
type udfGetter interface {
	GetUDFs() ([]*models.UDF, error)
}

// udfReader satisfies the DataReader interface
// It reads UDFs from a UDFGetter and returns them as *models.UDF
type udfReader struct {
	client udfGetter
	udfs   chan *models.UDF
	logger *slog.Logger
}

func newUDFReader(client udfGetter, logger *slog.Logger) *udfReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeUDF)
	logger.Debug("created new udf reader")

	return &udfReader{
		client: client,
		logger: logger,
	}
}

// Read reads the next UDF from the UDFGetter
func (r *udfReader) Read() (*models.Token, error) {
	// grab all the UDFs on the first run
	if r.udfs == nil {
		r.logger.Debug("fetching all UDFs")

		udfs, err := r.client.GetUDFs()
		if err != nil {
			return nil, err
		}

		r.udfs = make(chan *models.UDF, len(udfs))
		for _, udf := range udfs {
			r.udfs <- udf
		}
	}

	if len(r.udfs) > 0 {
		UDFToken := models.NewUDFToken(<-r.udfs, 0)
		return UDFToken, nil
	}

	return nil, io.EOF
}

// Close satisfies the DataReader interface
// but is a no-op for the UDFReader
func (r *udfReader) Close() {}
