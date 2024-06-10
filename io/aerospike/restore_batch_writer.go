package aerospike

import (
	"errors"
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

// restoreWriter satisfies the DataWriter interface
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type restoreBatchWriter struct {
	asc             dbWriter
	writePolicy     *a.WritePolicy
	stats           *models.RestoreStats
	logger          *slog.Logger
	operationBuffer []a.BatchRecordIfc
	batchSize       int
}

// NewRestoreWriter creates a new RestoreBatchWriter
func NewRestoreBatchWriter(asc dbWriter, writePolicy *a.WritePolicy,
	stats *models.RestoreStats, logger *slog.Logger) pipeline.DataWriter[*models.Token] {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeRestore)
	logger.Debug("created new restore writer")

	return &restoreBatchWriter{
		asc:         asc,
		writePolicy: writePolicy,
		stats:       stats,
		logger:      logger,
		batchSize:   10,
	}
}

// Write writes the types from the models package to an Aerospike DB.
// TODO support batch writes
func (rw *restoreBatchWriter) Write(data *models.Token) (int, error) {
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

func (rw *restoreBatchWriter) writeRecord(record *models.Record) error {
	writeOp := rw.batchWrite(record)
	rw.operationBuffer = append(rw.operationBuffer, writeOp)

	if len(rw.operationBuffer) > rw.batchSize {
		return rw.flushBuffer()
	}

	return nil
}

// batchWrite creates and returns a batch write operation for the record.
func (rw *restoreBatchWriter) batchWrite(r *models.Record) *a.BatchWrite {
	policy := a.NewBatchWritePolicy()
	policy.RecordExistsAction = rw.writePolicy.RecordExistsAction
	if rw.writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		policy.GenerationPolicy = a.EXPECT_GEN_GT
		policy.Generation = r.Generation
	}

	ops := make([]*a.Operation, 0, len(r.Bins))
	for k, v := range r.Bins {
		ops = append(ops, a.PutOp(a.NewBin(k, v)))
	}
	return a.NewBatchWrite(policy, r.Key, ops...)
}

// writeSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw *restoreBatchWriter) writeSecondaryIndex(si *models.SIndex) error {
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
func (rw *restoreBatchWriter) writeUDF(udf *models.UDF) error {
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
func (rw *restoreBatchWriter) Close() error {
	rw.logger.Debug("closed restore writer")
	return rw.flushBuffer()
}

func (rw *restoreBatchWriter) flushBuffer() error {
	err := rw.asc.BatchOperate(nil, rw.operationBuffer)
	if err != nil {
		return err
	}
	for i := 0; i < len(rw.operationBuffer); i++ {
		switch rw.operationBuffer[i].BatchRec().ResultCode {
		case atypes.OK:
			rw.stats.IncrRecordsInserted()
		case atypes.GENERATION_ERROR:
			rw.stats.IncrRecordsFresher()
		case atypes.KEY_EXISTS_ERROR:
			rw.stats.IncrRecordsExisted()
		}
	}

	rw.operationBuffer = nil
	return nil
}
