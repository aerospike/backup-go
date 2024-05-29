package aerospike

import (
	"errors"
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"

	// "github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

// restoreWriter satisfies the DataWriter interface
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type restoreWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	stats       *models.RestoreStats
	logger      *slog.Logger
}

// NewRestoreWriter creates a new RestoreWriter
func NewRestoreWriter(asc dbWriter, writePolicy *a.WritePolicy,
	stats *models.RestoreStats, logger *slog.Logger) pipeline.DataWriter[*models.Token] {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeRestore)
	logger.Debug("created new restore writer")

	return &restoreWriter{
		asc:         asc,
		writePolicy: writePolicy,
		stats:       stats,
		logger:      logger,
	}
}

// Write writes the types from the models package to an Aerospike DB.
// TODO support batch writes
func (rw restoreWriter) Write(data *models.Token) (int, error) {
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

func (rw restoreWriter) writeRecord(record *models.Record) error {
	writePolicy := rw.writePolicy
	if rw.writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		setGenerationPolicy := *rw.writePolicy
		setGenerationPolicy.Generation = record.Generation
		writePolicy = &setGenerationPolicy
	}

	aerr := rw.asc.Put(writePolicy, record.Key, record.Bins)
	if aerr != nil {
		if aerr.Matches(atypes.GENERATION_ERROR) {
			rw.stats.IncrRecordsFresher()
			return nil
		}

		if aerr.Matches(atypes.KEY_EXISTS_ERROR) {
			rw.stats.IncrRecordsExisted()
			return nil
		}

		rw.logger.Error("error writing record", "record", record.Key.Digest(), "error", aerr)

		return aerr
	}

	rw.stats.IncrRecordsInserted()

	return nil
}

// writeSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw restoreWriter) writeSecondaryIndex(si *models.SIndex) error {
	var sindexType a.IndexType

	switch si.Path.BinType {
	case models.NumericSIDataType:
		sindexType = a.NUMERIC
	case models.StringSIDataType:
		sindexType = a.STRING
	case models.BlobSIDataType:
		sindexType = a.BLOB
	case models.GEO2DSphereSIDataType:
		sindexType = a.GEO2DSPHERE
	default:
		return fmt.Errorf("invalid sindex bin type: %c", si.Path.BinType)
	}

	var sindexCollectionType a.IndexCollectionType

	switch si.IndexType {
	case models.BinSIndex:
		sindexCollectionType = a.ICT_DEFAULT
	case models.ListElementSIndex:
		sindexCollectionType = a.ICT_LIST
	case models.MapKeySIndex:
		sindexCollectionType = a.ICT_MAPKEYS
	case models.MapValueSIndex:
		sindexCollectionType = a.ICT_MAPVALUES
	default:
		return fmt.Errorf("invalid sindex collection type: %c", si.IndexType)
	}

	var ctx []*a.CDTContext

	if si.Path.B64Context != "" {
		var err error
		ctx, err = a.Base64ToCDTContext(si.Path.B64Context)

		if err != nil {
			rw.logger.Error("error decoding sindex context", "context", si.Path.B64Context, "error", err)
			return err
		}
	}

	job, err := rw.asc.CreateComplexIndex(
		rw.writePolicy,
		si.Namespace,
		si.Set,
		si.Name,
		si.Path.BinName,
		sindexType,
		sindexCollectionType,
		ctx...,
	)
	if err != nil {
		// if the sindex already exists, replace it because
		// the seconday index may have changed since the backup was taken
		if err.Matches(atypes.INDEX_FOUND) {
			rw.logger.Debug("index already exists, replacing it", "sindex", si.Name)

			err = rw.asc.DropIndex(rw.writePolicy, si.Namespace, si.Set, si.Name)
			if err != nil {
				rw.logger.Error("error dropping sindex", "sindex", si.Name, "error", err)
				return err
			}

			job, err = rw.asc.CreateComplexIndex(
				rw.writePolicy,
				si.Namespace,
				si.Set,
				si.Name,
				si.Path.BinName,
				sindexType,
				sindexCollectionType,
				ctx...,
			)
			if err != nil {
				rw.logger.Error("error creating replacement sindex", "sindex", si.Name, "error", err)
				return err
			}
		} else {
			rw.logger.Error("error creating sindex", "sindex", si.Name, "error", err)
			return err
		}
	}

	if job == nil {
		msg := "error creating sindex: job is nil"
		rw.logger.Debug(msg, "sindex", si.Name)

		return errors.New(msg)
	}

	errs := job.OnComplete()

	err = <-errs
	if err != nil {
		rw.logger.Error("error creating sindex", "sindex", si.Name, "error", err)
		return err
	}

	rw.logger.Debug("created sindex", "sindex", si.Name)

	return nil
}

// writeUDF writes a UDF to Aerospike
// TODO check that this does not overwrite existing UDFs
// TODO support write policy
func (rw restoreWriter) writeUDF(udf *models.UDF) error {
	var UDFLang a.Language

	switch udf.UDFType {
	case models.UDFTypeLUA:
		UDFLang = a.LUA
	default:
		msg := "error registering UDF: invalid UDF language"
		rw.logger.Debug(msg, "udf", udf.Name, "language", udf.UDFType)

		return errors.New(msg)
	}

	job, aerr := rw.asc.RegisterUDF(rw.writePolicy, udf.Content, udf.Name, UDFLang)
	if aerr != nil {
		rw.logger.Error("error registering UDF", "udf", udf.Name, "error", aerr)
		return aerr
	}

	if job == nil {
		msg := "error registering UDF: job is nil"
		rw.logger.Debug(msg, "udf", udf.Name)

		return errors.New(msg)
	}

	errs := job.OnComplete()

	err := <-errs
	if err != nil {
		rw.logger.Error("error registering UDF", "udf", udf.Name, "error", err)
		return err
	}

	rw.logger.Debug("registered UDF", "udf", udf.Name)

	return nil
}

// Close satisfies the DataWriter interface
// but is a no-op for the RestoreWriter
func (rw restoreWriter) Close() {
	rw.logger.Debug("closed restore writer")
}
