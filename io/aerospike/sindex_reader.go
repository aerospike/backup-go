package aerospike

import (
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// sindexGetter is an interface for getting secondary indexes
//
//go:generate mockery --name sindexGetter
type sindexGetter interface {
	GetSIndexes(namespace string) ([]*models.SIndex, error)
}

// sindexReader satisfies the DataReader interface
// It reads secondary indexes from a SIndexGetter and returns them as *models.SecondaryIndex
type sindexReader struct {
	client    sindexGetter
	sindexes  chan *models.SIndex
	logger    *slog.Logger
	namespace string
}

// NewSIndexReader creates a new SIndexReader
func NewSIndexReader(client sindexGetter, namespace string, logger *slog.Logger) *sindexReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeSIndex)
	logger.Debug("created new sindex reader")

	return &sindexReader{
		client:    client,
		namespace: namespace,
		logger:    logger,
	}
}

// Read reads the next secondary index from the SIndexGetter
func (r *sindexReader) Read() (*models.Token, error) {
	// grab all the sindexes on the first run
	if r.sindexes == nil {
		r.logger.Debug("fetching all secondary indexes")

		sindexes, err := r.client.GetSIndexes(r.namespace)
		if err != nil {
			return nil, err
		}

		r.sindexes = make(chan *models.SIndex, len(sindexes))
		for _, sindex := range sindexes {
			r.sindexes <- sindex
		}
	}

	if len(r.sindexes) > 0 {
		SIToken := models.NewSIndexToken(<-r.sindexes, 0)
		return SIToken, nil
	}

	return nil, io.EOF
}

// Close satisfies the DataReader interface
// but is a no-op for the SIndexReader
func (r *sindexReader) Close() {}
