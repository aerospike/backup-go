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
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/xdr"
	"github.com/aerospike/backup-go/models"
)

type payloadWriter struct {
	ctx               context.Context
	dbWriter          dbWriter
	writePolicy       *a.WritePolicy
	stats             *models.RestoreStats
	retryPolicy       *models.RetryPolicy
	metrics           *metrics.Collector
	logger            *slog.Logger
	ignoreRecordError bool
}

func newPayloadWriter(
	ctx context.Context,
	dbWriter dbWriter,
	writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	retryPolicy *models.RetryPolicy,
	metricsCollector *metrics.Collector,
	ignoreRecordError bool,
	logger *slog.Logger,
) *payloadWriter {
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	return &payloadWriter{
		ctx:               ctx,
		dbWriter:          dbWriter,
		writePolicy:       writePolicy,
		stats:             stats,
		retryPolicy:       retryPolicy,
		metrics:           metricsCollector,
		ignoreRecordError: ignoreRecordError,
		logger:            logger,
	}
}

func (p *payloadWriter) writePayload(t *models.ASBXToken) error {
	p.metrics.Increment()

	t.Payload = xdr.SetGenerationBit(p.writePolicy.GenerationPolicy, t.Payload)
	t.Payload = xdr.SetRecordExistsActionBit(p.writePolicy.RecordExistsAction, t.Payload)

	return p.retryPolicy.Do(p.ctx, func() error {
		aerr := p.dbWriter.PutPayload(p.writePolicy, t.Key, t.Payload)

		if aerr != nil && aerr.IsInDoubt() {
			p.stats.IncrErrorsInDoubt()
		}

		switch {
		case isNilOrAcceptableError(aerr):
			switch {
			case aerr == nil:
				p.stats.IncrRecordsInserted()
			case aerr.Matches(atypes.GENERATION_ERROR):
				p.stats.IncrRecordsFresher()
			case aerr.Matches(atypes.KEY_EXISTS_ERROR):
				p.stats.IncrRecordsExisted()
			}

			return nil
		case p.ignoreRecordError && shouldIgnore(aerr):
			p.stats.IncrRecordsIgnored()

			return nil
		case shouldRetry(aerr):
			p.stats.IncrRetryPolicyAttempts()

			return aerr
		default:
			// Retry on unknown errors.
			p.logger.Warn("Retrying unknown error", slog.Any("error", aerr))
			return aerr
		}
	})
}
