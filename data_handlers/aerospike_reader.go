package datahandlers

import (
	"backuplib/models"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type ARConfig struct {
	Namespace      string
	Set            string
	FirstPartition int
	NumPartitions  int
	First          bool
}

type ARStatus struct {
	partitionFilter *a.PartitionFilter
	started         bool
	scanPolicy      *a.ScanPolicy
}

type AerospikeReader struct {
	config     *ARConfig
	status     *ARStatus
	client     *a.Client
	recResChan <-chan *a.Result
	recSet     *a.Recordset
}

func NewAerospikeReader(cfg *ARConfig, client *a.Client) *AerospikeReader {
	job := &AerospikeReader{
		config:     cfg,
		client:     client,
		status:     &ARStatus{},
		recResChan: nil,
	}

	return job
}

func (j *AerospikeReader) Read() (any, error) {
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

func (j *AerospikeReader) Cancel() error {
	j.status.started = false
	return j.recSet.Close()
}

// **** Helper Functions

func startScan(j *AerospikeReader) (<-chan *a.Result, error) {

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

// TODO UDFs and SIndexes (one shot work)
