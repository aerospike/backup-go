package aerospike

import (
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/util"
)

type recordSets struct {
	logger *slog.Logger
	data   []*a.Recordset
}

func newRecordSets(data []*a.Recordset, logger *slog.Logger) *recordSets {
	return &recordSets{logger: logger, data: data}
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
	resultChannels := make([]<-chan *a.Result, 0, len(r.data))
	for _, recSet := range r.data {
		resultChannels = append(resultChannels, recSet.Results())
	}

	return util.MergeChannels(resultChannels)
}
