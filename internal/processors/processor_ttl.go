package processors

import (
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// expirationSetter is a DataProcessor that sets the Expiration (TTL) of a record based on its VoidTime.
// It is used during restore to set the TTL of records from their backed up VoidTime.
type expirationSetter struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow  func() cltime.CLTime
	expired *atomic.Uint64
	logger  *slog.Logger
}

// NewExpirationSetter creates a new expirationSetter processor
func NewExpirationSetter(expired *atomic.Uint64, logger *slog.Logger) TokenProcessor {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeTTL)
	logger.Debug("created new TTL processor")

	return &expirationSetter{
		getNow:  cltime.Now,
		expired: expired,
		logger:  logger,
	}
}

// errExpiredRecord is returned when a record is expired
// by embedding errFilteredOut, the processor worker will filter out the token
// containing the expired record
var errExpiredRecord = fmt.Errorf("%w: record is expired", errFilteredOut)

// Process sets the TTL of a record based on its VoidTime
func (p *expirationSetter) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	record := &token.Record
	now := p.getNow()

	switch {
	case record.VoidTime > 0:
		ttl := record.VoidTime - now.Seconds
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

	return token, nil
}
