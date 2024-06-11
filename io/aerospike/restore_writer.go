package aerospike

import (
	"errors"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

type recordWriter interface {
	writeRecord(record *models.Record) error
	close() error
}

// restoreWriter satisfies the DataWriter interface
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type restoreWriter struct {
	sindexWriter
	udfWriter
	recordWriter
	logger *slog.Logger
}

// NewRestoreWriter creates a new RestoreWriter
func NewRestoreWriter(asc dbWriter, writePolicy *a.WritePolicy, stats *models.RestoreStats,
	logger *slog.Logger, disableBatch bool, batchSize int) pipeline.DataWriter[*models.Token] {
	logger = logging.WithWriter(logger, uuid.NewString(), logging.WriterTypeRestore)
	logger.Debug("created new restore writer")

	return &restoreWriter{
		sindexWriter: sindexWriter{
			asc:         asc,
			writePolicy: writePolicy,
			logger:      logger,
		},
		udfWriter: udfWriter{
			asc:         asc,
			writePolicy: writePolicy,
			logger:      logger,
		},
		recordWriter: newRecordWriter(asc, writePolicy, stats, logger, disableBatch, batchSize),
		logger:       logger,
	}
}

func newRecordWriter(asc dbWriter, writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	logger *slog.Logger,
	disableBatch bool,
	batchSize int,
) recordWriter {
	if disableBatch {
		return &singleRecordWriter{
			asc:         asc,
			writePolicy: writePolicy,
			stats:       stats,
			logger:      logger,
		}
	}

	return &recordBatchWriter{
		asc:         asc,
		writePolicy: writePolicy,
		stats:       stats,
		logger:      logger,
		batchSize:   batchSize,
	}
}

// Write writes the types from the models package to an Aerospike DB.
func (rw *restoreWriter) Write(data *models.Token) (int, error) {
	switch data.Type {
	case models.TokenTypeRecord:
		return int(data.Size), rw.writeRecord(&data.Record)
	case models.TokenTypeUDF:
		return int(data.Size), rw.writeUDF(data.UDF)
	case models.TokenTypeSIndex:
		return int(data.Size), rw.writeSecondaryIndex(data.SIndex)
	case models.TokenTypeInvalid:
		return 0, errors.New("invalid token")
	default:
		return 0, errors.New("unsupported token type")
	}
}

// Close satisfies the DataWriter interface
// but is a no-op for the RestoreWriter
func (rw *restoreWriter) Close() error {
	rw.logger.Debug("closed restore writer")
	return rw.close()
}
