package aerospike

import (
	"errors"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

type udfWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	logger      *slog.Logger
}

// writeUDF writes a UDF to Aerospike
// TODO check that this does not overwrite existing UDFs
// TODO support write policy
func (rw udfWriter) writeUDF(udf *models.UDF) error {
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
