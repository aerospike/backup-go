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
	"context"
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

type singleRecordWriter struct {
	ctx               context.Context
	asc               dbWriter
	writePolicy       *a.WritePolicy
	stats             *models.RestoreStats
	retryPolicy       *models.RetryPolicy
	rpsCollector      *metrics.Collector
	logger            *slog.Logger
	ignoreRecordError bool
}

func newSingleRecordWriter(
	ctx context.Context,
	asc dbWriter,
	writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	retryPolicy *models.RetryPolicy,
	rpsCollector *metrics.Collector,
	ignoreRecordError bool,
	logger *slog.Logger,
) *singleRecordWriter {
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	return &singleRecordWriter{
		ctx:               ctx,
		asc:               asc,
		writePolicy:       writePolicy,
		stats:             stats,
		retryPolicy:       retryPolicy,
		rpsCollector:      rpsCollector,
		ignoreRecordError: ignoreRecordError,
		logger:            logger,
	}
}

func (rw *singleRecordWriter) writeRecord(record *models.Record) error {
	uuid, _ := uuid.NewRandom()
	fmt.Printf("\nWrite RECORD TOKEN:%s:%+v\n", uuid, record)
	defer fmt.Println("UDF RECORD TOKEN:", uuid)
	// To prevent data race, we must create copy of value.
	writePolicy := *rw.writePolicy
	if rw.writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		setGenerationPolicy := *rw.writePolicy
		setGenerationPolicy.Generation = record.Generation
		writePolicy = setGenerationPolicy
	}

	writePolicy.Expiration = record.Expiration

	rw.rpsCollector.Increment()

	err := rw.executeWrite(&writePolicy, record)
	if err != nil {
		return fmt.Errorf("error writing record %s: %w", record.Key.Digest(), err)
	}

	return nil
}

func (rw *singleRecordWriter) executeWrite(writePolicy *a.WritePolicy, record *models.Record) error {
	return rw.retryPolicy.Do(rw.ctx, func() error {
		aerr := rw.asc.Put(writePolicy, record.Key, record.Bins)

		if aerr != nil && aerr.IsInDoubt() {
			rw.stats.IncrErrorsInDoubt()
		}

		switch {
		case isNilOrAcceptableError(aerr):
			switch {
			case aerr == nil:
				rw.stats.IncrRecordsInserted()
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
			rw.stats.IncrRetryPolicyAttempts()

			return aerr
		default:
			// Retry on unknown errors.
			rw.logger.Warn("Retrying unknown error", slog.Any("error", aerr))
			return aerr
		}
	})
}

func (rw *singleRecordWriter) close() error {
	fmt.Println("------ CLOSE RECORD WRITER")
	return nil
}
