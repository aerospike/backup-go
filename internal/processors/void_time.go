package processors

import (
	"log/slog"

	"github.com/aerospike/backup-go/internal/citrusleaf_time"
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

	record := &token.Record
	now := p.getNow()

	if record.Expiration == models.ExpirationNever {
		record.VoidTime = models.VoidTimeNeverExpire
	} else {
		record.VoidTime = now.Seconds + int64(record.Expiration)
	}

	return token, nil
}
