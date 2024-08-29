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

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/models"
)

type singleRecordWriter struct {
	asc               dbWriter
	writePolicy       *a.WritePolicy
	stats             *models.RestoreStats
	retryPolicy       *models.RetryPolicy
	ignoreRecordError bool
}

func (rw *singleRecordWriter) writeRecord(record *models.Record) error {
	writePolicy := rw.writePolicy
	if rw.writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		setGenerationPolicy := *rw.writePolicy
		setGenerationPolicy.Generation = record.Generation
		writePolicy = &setGenerationPolicy
	}

	writePolicy.Expiration = record.Expiration

	err := rw.executeWrite(writePolicy, record)
	if err != nil {
		return fmt.Errorf("error writing record %s: %w", record.Key.Digest(), err)
	}

	return nil
}

func (rw *singleRecordWriter) executeWrite(writePolicy *a.WritePolicy, record *models.Record) error {
	var (
		aerr    a.Error
		attempt uint
	)

	for attemptsLeft(rw.retryPolicy, attempt) {
		aerr = rw.asc.Put(writePolicy, record.Key, record.Bins)
		if aerr == nil {
			rw.stats.IncrRecordsInserted()

			return nil
		}

		switch {
		case isNilOrAcceptableError(aerr):
			switch {
			case aerr.Matches(atypes.GENERATION_ERROR):
				rw.stats.IncrRecordsFresher()
			case aerr.Matches(atypes.KEY_EXISTS_ERROR):
				rw.stats.IncrRecordsExisted()
			}

			return nil

		case rw.ignoreRecordError && shouldIgnore(aerr):
			rw.stats.IncrRecordsIgnored()
			return nil

		case shouldRetry(aerr):
			sleep(rw.retryPolicy, attempt)

			attempt++

			continue
		}

		return aerr
	}

	return aerr
}

func (rw *singleRecordWriter) close() error {
	return nil
}
