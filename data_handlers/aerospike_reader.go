package datahandlers

import (
	"backuplib/models"

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
	backupStarted   bool
	scanPolicy      *a.ScanPolicy
}

type AerospikeReader struct {
	config     *ARConfig
	status     *ARStatus
	client     *a.Client
	recResChan <-chan *a.Result
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

	// TODO do single shot work
	// if j.status.first {
	// }

	if !j.status.backupStarted {
		var err error
		j.recResChan, err = startScan(j)
		if err != nil {
			return nil, err
		}
		j.status.backupStarted = true
	}

	res := <-j.recResChan
	if res.Err != nil {
		return nil, res.Err
	}

	rec := (*models.Record)(res.Record)

	return rec, nil
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

	return recSet.Results(), nil
}

// TODO UDFs and SIndexes (one shot work)
