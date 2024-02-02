package datahandlers

import (
	"backuplib/models"
	"errors"
	"fmt"

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

// TODO check that this does not overwrite existing sindexes
// WriteSecondaryIndex writes a secondary index to Aerospike
func (rw *RestoreWriter) WriteSecondaryIndex(si *models.SecondaryIndex) error {
	// SIndexes always have one path for now
	if len(si.Paths) != 1 {
		return errors.New("invalid number of sindex paths")
	}

	var sindexType a.IndexType
	switch si.Paths[0].BinType {
	case models.NumericSIDataType:
		sindexType = a.NUMERIC
	case models.StringSIDataType:
		sindexType = a.STRING
	case models.BlobSIDataType:
		sindexType = a.BLOB
	case models.GEO2DSphereSIDataType:
		sindexType = a.GEO2DSPHERE
	default:
		return nil
	}

	binName := si.Paths[0].BinName
	_, err := rw.asc.CreateIndex(nil, si.Namespace, si.Set, si.Name, binName, sindexType)

	return err
}

// WriteUDF writes a UDF to Aerospike
// TODO check that this does not overwrite existing UDFs
func (rw *RestoreWriter) WriteUDF(udf *models.UDF) error {

	var UDFLang a.Language
	switch udf.UDFType {
	case models.LUAUDFType:
		UDFLang = a.LUA
	default:
		return fmt.Errorf("invalid UDF language: %c", udf.UDFType)
	}

	_, err := rw.asc.RegisterUDF(nil, udf.Content, udf.Name, UDFLang)
	return err
}

type RestoreWriterFactory struct {
	asc *a.Client
}

func NewRestoreWriterFactory(a *a.Client) *RestoreWriterFactory {
	return &RestoreWriterFactory{
		asc: a,
	}
}

func (rwf *RestoreWriterFactory) CreateWriter() DataWriter {
	return NewRestoreWriter(rwf.asc)
}
