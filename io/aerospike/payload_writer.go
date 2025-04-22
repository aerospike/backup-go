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

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/xdr"
	"github.com/aerospike/backup-go/models"
)

type payloadWriter struct {
	dbWriter          dbWriter
	writePolicy       *a.WritePolicy
	stats             *models.RestoreStats
	retryPolicy       *models.RetryPolicy
	metrics           *metrics.RPSCollector
	ignoreRecordError bool
}

func (p *payloadWriter) writePayload(t *models.ASBXToken) error {
	var (
		aerr    a.Error
		attempt uint
	)

	p.metrics.Increment()

	t.Payload = xdr.SetGenerationBit(p.writePolicy.GenerationPolicy, t.Payload)
	t.Payload = xdr.SetRecordExistsActionBit(p.writePolicy.RecordExistsAction, t.Payload)

	for attemptsLeft(p.retryPolicy, attempt) {
		aerr = p.dbWriter.PutPayload(p.writePolicy, t.Key, t.Payload)

		if aerr == nil {
			p.stats.IncrRecordsInserted()

			return nil
		}

		if aerr.IsInDoubt() {
			p.stats.IncrErrorsInDoubt()
		}

		switch {
		case isNilOrAcceptableError(aerr):
			switch {
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
			sleep(p.retryPolicy, attempt)

			attempt++

			continue
		}

		return fmt.Errorf("failed to write payload: %w", aerr)
	}

	return aerr
}
