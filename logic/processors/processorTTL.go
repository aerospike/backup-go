package processors

import (
	"log/slog"
	"sync/atomic"

	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/logic/logging"
	"github.com/google/uuid"
)

// processorTTL is a DataProcessor that sets the TTL of a record based on its VoidTime.
// It is used during restore to set the TTL of records from their backed up VoidTime.
type processorTTL struct {
	// getNow returns the current time since the citrusleaf epoch
	// It is a field so that it can be mocked in tests
	getNow  func() cltime.CLTime
	expired *atomic.Uint64
	logger  *slog.Logger
}

// NewProcessorTTL creates a new TTLProcessor
func NewProcessorTTL(expired *atomic.Uint64, logger *slog.Logger) TokenProcessor {
	id := uuid.NewString()
	logger = logging.WithProcessor(logger, id, logging.ProcessorTypeTTL)
	logger.Debug("created new TTL processor")

	return &processorTTL{
		getNow:  cltime.Now,
		expired: expired,
		logger:  logger,
	}
}
