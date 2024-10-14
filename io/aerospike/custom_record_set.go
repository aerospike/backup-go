package aerospike

import (
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

type customResult struct {
	Result *a.Result
	Filter *models.PartitionFilterSerialized
}

func newCustomResult(result *a.Result, filter *models.PartitionFilterSerialized) *customResult {
	return &customResult{
		Result: result,
		Filter: filter,
	}
}

// recordSets contains multiple Aerospike Recordset objects.
type customRecordSets struct {
	resultsChannel <-chan *customResult
	logger         *slog.Logger
	data           []*scanResult
}

func newCustomRecordSets(data []*scanResult, logger *slog.Logger) *customRecordSets {
	out := make(chan *customResult)
	go streamData(data, out)

	return &customRecordSets{
		resultsChannel: out,
		data:           data,
		logger:         logger,
	}
}

// Results returns the results channel of the recordSets.
func (r *customRecordSets) Results() <-chan *customResult {
	return r.resultsChannel
}

func streamData(data []*scanResult, out chan *customResult) {
	if len(data) == 0 {
		close(out)
	}

	for _, d := range data {
	
		for _, n := range d.records {

			out <- newCustomResult(n, &d.Filter)
		}
	}

	close(out)
}
