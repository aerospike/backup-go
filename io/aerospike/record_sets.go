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
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/util"
)

// recordSets contains multiple Aerospike Recordset objects.
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
			r.logger.Error("error while closing record set", "error", err)
		}
	}
}

// Results returns the results channel of the recordSets.
func (r *recordSets) Results() <-chan *a.Result {
	return r.resultsChannel
}
