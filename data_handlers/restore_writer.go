package datahandlers

import (
	"backuplib/models"
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type RestoreWriter struct {
	asc *a.Client
}

// NewRestoreWriter creates a new RestoreWriter
func NewRestoreWriter(asc *a.Client) *RestoreWriter {
	return &RestoreWriter{
		asc: asc,
	}
}

// Write writes the data
// TODO support write policy
// TODO support batch writes
func (rw *RestoreWriter) Write(data any) error {
	switch d := data.(type) {
	case *models.Record:
		return rw.asc.Put(nil, d.Key, d.Bins)
	case *models.UDF:
		return rw.WriteUDF(d)
	case *models.SecondaryIndex:
		return rw.WriteSecondaryIndex(d)
	default:
		return nil
	}
}

// WriteSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw *RestoreWriter) WriteSecondaryIndex(si *models.SecondaryIndex) error {

	// var sindexType a.IndexType
	// switch si.Path.BinType {
	// case models.NumericSIDataType:
	// 	sindexType = a.NUMERIC
	// case models.StringSIDataType:
	// 	sindexType = a.STRING
	// case models.BlobSIDataType:
	// 	sindexType = a.BLOB
	// case models.GEO2DSphereSIDataType:
	// 	sindexType = a.GEO2DSPHERE
	// default:
	// 	return nil
	// }

	// binName := si.Path.BinName
	// _, err := rw.asc.CreateIndex(nil, si.Namespace, si.Set, si.Name, binName, sindexType)

	// switch {
	// case err.Matches(atypes.INDEX_FOUND):
	// 	//TODO compare sindexes
	// }

	return errors.New("UNIMPLEMENTED")
}

// WriteUDF writes a UDF to Aerospike
// TODO check that this does not overwrite existing UDFs
// TODO support write policy
func (rw *RestoreWriter) WriteUDF(udf *models.UDF) error {

	// var UDFLang a.Language
	// switch udf.UDFType {
	// case models.LUAUDFType:
	// 	UDFLang = a.LUA
	// default:
	// 	return fmt.Errorf("invalid UDF language: %c", udf.UDFType)
	// }

	// _, err := rw.asc.RegisterUDF(nil, udf.Content, udf.Name, UDFLang)
	// return err

	return errors.New("UNIMPLEMENTED")
}

func (rw *RestoreWriter) Cancel() error {
	return nil
}
