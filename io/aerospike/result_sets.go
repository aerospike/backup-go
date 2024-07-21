package aerospike

import (
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/util"
)

// recordSets contains multiple Aerospike Recordset objects
type recordSets struct {
	resultsChannel <-chan *a.Result
	logger         *slog.Logger
	data           []*a.Recordset
}

func newRecordSets(data []*a.Recordset, logger *slog.Logger) *recordSets {
	resultChannels := make([]<-chan *a.Result, 0, len(data))
	for _, recSet := range data {
		resultChannels = append(resultChannels, recSet.Results())
	}

	return &recordSets{
		resultsChannel: util.MergeChannels(resultChannels),
		data:           data,
		logger:         logger,
	}
}

func (r *recordSets) Close() {
	for _, rec := range r.data {
		if err := rec.Close(); err != nil {
			// ignore this error, it only happens if the scan is already closed
			// and this method can not return an error anyway
			r.logger.Error("error while closing record set", "error", rec.Close())
		}
	}
}

func (r *recordSets) Results() <-chan *a.Result {
	return r.resultsChannel
}
