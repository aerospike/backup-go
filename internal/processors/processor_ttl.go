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

package processors

import (
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"

	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

// expirationSetter is a DataProcessor that sets the Expiration (TTL) of a record based on its VoidTime.
// It is used during restore to set the TTL of records from their backed up VoidTime.
type expirationSetter[T models.TokenConstraint] struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow   func() cltime.CLTime
	expired  *atomic.Uint64
	extraTTL int64
	logger   *slog.Logger
}

// NewExpirationSetter creates a new expirationSetter processor
func NewExpirationSetter[T models.TokenConstraint](expired *atomic.Uint64, extraTTL int64, logger *slog.Logger,
) pipeline.DataProcessor[T] {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeTTL)
	logger.Debug("created new TTL processor")

	return &expirationSetter[T]{
		getNow:   cltime.Now,
		expired:  expired,
		extraTTL: extraTTL,
		logger:   logger,
	}
}

// errExpiredRecord is returned when a record is expired
// by embedding errFilteredOut, the processor worker will filter out the token
// containing the expired record
var errExpiredRecord = fmt.Errorf("%w: record is expired", pipeline.ErrFilteredOut)

// Process sets the TTL of a record based on its VoidTime
func (p *expirationSetter[T]) Process(token T) (T, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type for ttl")
	}
	// if the token is not a record, we don't need to process it
	if t.Type != models.TokenTypeRecord {
		return token, nil
	}

	record := t.Record
	now := p.getNow()

	switch {
	case record.VoidTime > 0:
		ttl := record.VoidTime - now.Seconds + p.extraTTL
		if ttl <= 0 {
			// the record is expired
			p.logger.Debug("record is expired", "digest", record.Key.Digest())
			p.expired.Add(1)

			return nil, errExpiredRecord
		}

		if ttl > math.MaxUint32 {
			return nil, fmt.Errorf("calculated TTL %d is too large", ttl)
		}

		record.Expiration = uint32(ttl)
	case record.VoidTime == 0:
		record.Expiration = models.ExpirationNever
	default:
		return nil, fmt.Errorf("invalid void time %d", record.VoidTime)
	}

	return any(t).(T), nil
}
