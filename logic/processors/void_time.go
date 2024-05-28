package processors

import (
	"log/slog"

	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/logic/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// processorVoidTime is a DataProcessor that sets the VoidTime of a record based on its TTL
// It is used during backup to set the VoidTime of records from their TTL
// The VoidTime is the time at which the record will expire and is usually what is encoded in backups
type processorVoidTime struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow func() cltime.CLTime
	logger *slog.Logger
}

// NewProcessorVoidTime creates a new VoidTimeProcessor
func NewProcessorVoidTime(logger *slog.Logger) TokenProcessor {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeVoidTime)
	logger.Debug("created new VoidTime processor")

	return &processorVoidTime{
		getNow: cltime.Now,
		logger: logger,
	}
}

// Process sets the VoidTime of a record based on its TTL
func (p *processorVoidTime) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	record := &token.Record
	now := p.getNow()

	if record.Expiration == models.ExpirationNever {
		record.VoidTime = models.VoidTimeNeverExpire
	} else {
		record.VoidTime = now.Seconds + int64(record.Expiration)
	}

	return token, nil
}
