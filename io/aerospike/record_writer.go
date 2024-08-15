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
	"fmt"
	"math"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/models"
)

type singleRecordWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	stats       *models.RestoreStats
}

func (rw *singleRecordWriter) writeRecord(record *models.Record) error {
	writePolicy := rw.writePolicy
	if rw.writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		setGenerationPolicy := *rw.writePolicy
		setGenerationPolicy.Generation = record.Generation
		writePolicy = &setGenerationPolicy
	}

	retries := writePolicy.MaxRetries

	var aerr a.Error
	for attempt := 0; attempt <= retries; attempt++ {
		aerr = rw.asc.Put(writePolicy, record.Key, record.Bins)
		if aerr == nil {
			rw.stats.IncrRecordsInserted()
			return nil
		}

		if aerr.Matches(atypes.GENERATION_ERROR) {
			rw.stats.IncrRecordsFresher()
			return nil
		}

		if aerr.Matches(atypes.KEY_EXISTS_ERROR) {
			rw.stats.IncrRecordsExisted()
			return nil
		}

		time.Sleep(time.Duration(math.Pow(2, float64(attempt))) * baseDelay)
	}

	return fmt.Errorf("max retries reached: error writing record %s: %w", record.Key.Digest(), aerr)
}

func (rw *singleRecordWriter) close() error {
	return nil
}
