package aerospike

import (
	"fmt"
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
		return fmt.Errorf("error registering UDF %s: invalid UDF language %b", udf.Name, udf.UDFType)
	}

	job, aerr := rw.asc.RegisterUDF(rw.writePolicy, udf.Content, udf.Name, UDFLang)
	if aerr != nil {
		return fmt.Errorf("error registering UDF %s: %w", udf.Name, aerr)
	}

	if job == nil {
		return fmt.Errorf("error registering UDF %s: job is nil", udf.Name)
	}

	errs := job.OnComplete()

	err := <-errs
	if err != nil {
		return fmt.Errorf("error registering UDF %s: %w", udf.Name, err)
	}

	rw.logger.Debug("registered UDF", "udf", udf.Name)

	return nil
}
