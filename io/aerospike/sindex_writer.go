package aerospike

import (
	"errors"
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/models"
)

type sindexWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	logger      *slog.Logger
}

// writeSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw sindexWriter) writeSecondaryIndex(si *models.SIndex) error {
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