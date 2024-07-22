package aerospike

import (
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// udfGetter is an interface for getting UDFs
//
//go:generate mockery --name udfGetter
type udfGetter interface {
	GetUDFs() ([]*models.UDF, error)
}

// UdfReader satisfies the DataReader interface
// It reads UDFs from a UDFGetter and returns them as *models.UDF
type UdfReader struct {
	client udfGetter
	udfs   chan *models.UDF
	logger *slog.Logger
}

func NewUDFReader(client udfGetter, logger *slog.Logger) *UdfReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeUDF)
	logger.Debug("created new udf reader")

	return &UdfReader{
		client: client,
		logger: logger,
	}
}

// Read reads the next UDF from the UDFGetter
func (r *UdfReader) Read() (*models.Token, error) {
	// grab all the UDFs on the first run
	if r.udfs == nil {
		r.logger.Debug("fetching all UDFs")

		udfs, err := r.client.GetUDFs()
		if err != nil {
			return nil, err
		}

		r.udfs = make(chan *models.UDF, len(udfs))
		for _, udf := range udfs {
			r.udfs <- udf
		}
	}

	if len(r.udfs) > 0 {
		UDFToken := models.NewUDFToken(<-r.udfs, 0)
		return UDFToken, nil
	}

	return nil, io.EOF
}

// Close satisfies the DataReader interface
// but is a no-op for the UDFReader
func (r *UdfReader) Close() {}
