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
	"log/slog"

	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// voidTimeSetter is a DataProcessor that sets the VoidTime of a record based on its TTL
// It is used during backup to set the VoidTime of records from their TTL
// The VoidTime is the time at which the record will expire and is usually what is encoded in backups
type voidTimeSetter struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow func() cltime.CLTime
	logger *slog.Logger
}

// NewVoidTimeSetter creates a new VoidTimeProcessor
func NewVoidTimeSetter(logger *slog.Logger) TokenProcessor {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeVoidTime)
	logger.Debug("created new VoidTime processor")

	return &voidTimeSetter{
		getNow: cltime.Now,
		logger: logger,
	}
}

// Process sets the VoidTime of a record based on its TTL
func (p *voidTimeSetter) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	record := token.Record
	now := p.getNow()

	if record.Expiration == models.ExpirationNever {
		record.VoidTime = models.VoidTimeNeverExpire
	} else {
		record.VoidTime = now.Seconds + int64(record.Expiration)
	}

	return token, nil
}
