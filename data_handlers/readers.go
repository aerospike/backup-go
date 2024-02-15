package datahandlers

import (
	"backuplib/models"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// readers.go contains the implementations of the DataReader interface
// used by dataPipelines in the backuplib package

// Decoder is an interface for reading backup data as tokens.
// It is used to support different data formats.
// While the return type is `any`, the actual types returned should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
//
//go:generate mockery --name Decoder
type Decoder interface {
	NextToken() (any, error)
}

// **** Generic Reader ****

// GenericReader satisfies the DataReader interface
// It reads data as tokens using a Decoder
type GenericReader struct {
	decoder Decoder
}

// NewGenericReader creates a new GenericReader
func NewGenericReader(decoder Decoder) *GenericReader {
	return &GenericReader{
		decoder: decoder,
	}
}

// Read reads the next token from the decoder
func (dr *GenericReader) Read() (any, error) {
	return dr.decoder.NextToken()
}

// Cancel satisfies the DataReader interface
// but is a no-op for the GenericReader
func (dr *GenericReader) Cancel() {}

// **** Aerospike DB Reader ****

// ARRConfig is the configuration for an AerospikeRecordReader
type ARRConfig struct {
	Namespace      string
	Set            string
	FirstPartition int
	NumPartitions  int
}

// ARRStatus is the status of an AerospikeRecordReader
type ARRStatus struct {
	partitionFilter *a.PartitionFilter
	started         bool
	scanPolicy      *a.ScanPolicy
}

// Scanner is an interface for scanning Aerospike records
// the Aerospike go client satisfies this interface
//
//go:generate mockery --name Scanner
type Scanner interface {
	ScanPartitions(*a.ScanPolicy, *a.PartitionFilter, string, string, ...string) (*a.Recordset, a.Error)
}

// AerospikeRecordReader satisfies the DataReader interface
// It reads records from an Aerospike database and returns them as *models.Record
type AerospikeRecordReader struct {
	config     *ARRConfig
	status     *ARRStatus
	client     Scanner
	recResChan <-chan *a.Result
	recSet     *a.Recordset
}

// NewAerospikeRecordReader creates a new AerospikeRecordReader
func NewAerospikeRecordReader(cfg *ARRConfig, client Scanner) *AerospikeRecordReader {
	job := &AerospikeRecordReader{
		config:     cfg,
		client:     client,
		status:     &ARRStatus{},
		recResChan: nil,
	}

	return job
}

// Read reads the next record from the Aerospike database
func (j *AerospikeRecordReader) Read() (any, error) {
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

	rec := (*models.Record)(res.Record)

	return rec, nil
}

// Cancel cancels the Aerospike scan used to read records
// if it was started
func (j *AerospikeRecordReader) Cancel() {
	j.status.started = false
	if j.recSet != nil {
		j.recSet.Close()
	}
}

func startScan(j *AerospikeRecordReader) (<-chan *a.Result, error) {

	j.recResChan = make(chan *a.Result)

	j.status.partitionFilter = a.NewPartitionFilterByRange(
		j.config.FirstPartition,
		j.config.NumPartitions,
	)

	policy := a.NewScanPolicy()
	j.status.scanPolicy = policy

	recSet, err := j.client.ScanPartitions(
		j.status.scanPolicy,
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

// SIndexGetter is an interface for getting secondary indexes
//
//go:generate mockery --name SIndexGetter
type SIndexGetter interface {
	GetSIndexes(namespace string) ([]*models.SIndex, error)
}

// SIndexReader satisfies the DataReader interface
// It reads secondary indexes from a SIndexGetter and returns them as *models.SecondaryIndex
type SIndexReader struct {
	client    SIndexGetter
	namespace string
	sindexes  chan *models.SIndex
}

// NewSIndexReader creates a new SIndexReader
func NewSIndexReader(client SIndexGetter, namespace string) *SIndexReader {
	return &SIndexReader{
		client:    client,
		namespace: namespace,
	}
}

// Read reads the next secondary index from the SIndexGetter
func (r *SIndexReader) Read() (any, error) {
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
		return <-r.sindexes, nil
	}

	return nil, io.EOF
}

// Cancel satisfies the DataReader interface
// but is a no-op for the SIndexReader
func (r *SIndexReader) Cancel() {}
