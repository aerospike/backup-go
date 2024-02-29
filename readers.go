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

package backuplib

import (
	"context"
	"errors"
	"io"

	"github.com/aerospike/aerospike-tools-backup-lib/models"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Read Worker ****

// dataReader is an interface for reading data from a source.
//
//go:generate mockery --name dataReader
type dataReader[T any] interface {
	Read() (T, error)
	Cancel()
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
	defer w.reader.Cancel()

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

// **** Generic Reader ****

// genericReader satisfies the DataReader interface
// It reads data as tokens using a Decoder
type genericReader struct {
	decoder Decoder
}

// newGenericReader creates a new GenericReader
func newGenericReader(decoder Decoder) *genericReader {
	return &genericReader{
		decoder: decoder,
	}
}

// Read reads the next token from the decoder
func (dr *genericReader) Read() (*models.Token, error) {
	data, err := dr.decoder.NextToken()
	if err != nil {
		return nil, err
	}

	switch data.Type {
	case models.TokenTypeRecord:
		return models.NewRecordToken(data.Record), nil
	case models.TokenTypeUDF:
		return models.NewUDFToken(data.UDF), nil
	case models.TokenTypeSIndex:
		return models.NewSIndexToken(data.SIndex), nil
	case models.TokenTypeInvalid:
		return nil, errors.New("invalid token")
	default:
		return nil, errors.New("unsupported token type")
	}
}

// Cancel satisfies the DataReader interface
// but is a no-op for the GenericReader
func (dr *genericReader) Cancel() {}

// **** Aerospike DB Reader ****

// arrConfig is the configuration for an AerospikeRecordReader
type arrConfig struct {
	Namespace      string
	Set            string
	FirstPartition int
	NumPartitions  int
}

// arrStatus is the status of an AerospikeRecordReader
type arrStatus struct {
	partitionFilter *a.PartitionFilter
	started         bool
}

// scanner is an interface for scanning Aerospike records
// the Aerospike go client satisfies this interface
//
//go:generate mockery --name scanner
type scanner interface {
	ScanPartitions(*a.ScanPolicy, *a.PartitionFilter, string, string, ...string) (*a.Recordset, a.Error)
}

// aerospikeRecordReader satisfies the DataReader interface
// It reads records from an Aerospike database and returns them as *models.Record
type aerospikeRecordReader struct {
	client     scanner
	scanPolicy *a.ScanPolicy
	recResChan <-chan *a.Result
	recSet     *a.Recordset
	status     arrStatus
	config     arrConfig
}

// newAerospikeRecordReader creates a new AerospikeRecordReader
func newAerospikeRecordReader(client scanner, cfg arrConfig, scanPolicy *a.ScanPolicy) *aerospikeRecordReader {
	job := &aerospikeRecordReader{
		config:     cfg,
		client:     client,
		status:     arrStatus{},
		scanPolicy: scanPolicy,
		recResChan: nil,
	}

	return job
}

// Read reads the next record from the Aerospike database
func (j *aerospikeRecordReader) Read() (*models.Token, error) {
	if !j.status.started {
		var err error

		j.recResChan, err = startScan(j)

		if err != nil {
			return nil, err
		}

		j.status.started = true
	}

	res, active := <-j.recResChan
	if !active {
		return nil, io.EOF
	}

	if res.Err != nil {
		return nil, res.Err
	}

	rec := res.Record
	recToken := models.NewRecordToken(rec)

	return recToken, nil
}

// Cancel cancels the Aerospike scan used to read records
// if it was started
func (j *aerospikeRecordReader) Cancel() {
	j.status.started = false
	if j.recSet != nil {
		// ignore this error, it only happens if the scan is already closed
		// and this method can not return an error anyway
		_ = j.recSet.Close()
	}
}

func startScan(j *aerospikeRecordReader) (<-chan *a.Result, error) {
	j.recResChan = make(chan *a.Result)

	j.status.partitionFilter = a.NewPartitionFilterByRange(
		j.config.FirstPartition,
		j.config.NumPartitions,
	)

	recSet, err := j.client.ScanPartitions(
		j.scanPolicy,
		j.status.partitionFilter,
		j.config.Namespace,
		j.config.Set,
	)
	if err != nil {
		return nil, err
	}

	j.recSet = recSet

	return recSet.Results(), nil
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
	namespace string
}

// newSIndexReader creates a new SIndexReader
func newSIndexReader(client sindexGetter, namespace string) *sindexReader {
	return &sindexReader{
		client:    client,
		namespace: namespace,
	}
}

// Read reads the next secondary index from the SIndexGetter
func (r *sindexReader) Read() (*models.Token, error) {
	// grab all the sindexes on the first run
	if r.sindexes == nil {
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
		SIToken := models.NewSIndexToken(<-r.sindexes)
		return SIToken, nil
	}

	return nil, io.EOF
}

// Cancel satisfies the DataReader interface
// but is a no-op for the SIndexReader
func (r *sindexReader) Cancel() {}
