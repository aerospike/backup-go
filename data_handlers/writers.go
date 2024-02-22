// Copyright 2024-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datahandlers

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/aerospike/aerospike-tools-backup-lib/models"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// writers.go contains the implementations of the DataWriter interface
// used by dataPipelines in the backuplib package

// **** Generic Writer ****

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
//
//go:generate mockery --name Encoder
type Encoder interface {
	EncodeRecord(v *models.Record) ([]byte, error)
	EncodeUDF(v *models.UDF) ([]byte, error)
	EncodeSIndex(v *models.SIndex) ([]byte, error)
}

// GenericWriter satisfies the DataWriter interface
// It writes the types from the models package as encoded data
// to an io.Writer. It uses an Encoder to encode the data.
// While the input type is `any`, the actual types written should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
type GenericWriter struct {
	encoder Encoder
	output  io.Writer
}

// NewGenericWriter creates a new GenericWriter
func NewGenericWriter(encoder Encoder, output io.Writer) *GenericWriter {
	return &GenericWriter{
		encoder: encoder,
		output:  output,
	}
}

// Write encodes v and writes it to the output
// TODO let the encoder handle the type checking
// TODO maybe restrict the types that can be written to this
func (w *GenericWriter) Write(v any) error {
	var (
		err  error
		data []byte
	)

	switch v := v.(type) {
	case *models.Record:
		data, err = w.encoder.EncodeRecord(v)
	case *models.UDF:
		data, err = w.encoder.EncodeUDF(v)
	case *models.SIndex:
		data, err = w.encoder.EncodeSIndex(v)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}

	if err != nil {
		return err
	}

	_, err = w.output.Write(data)
	return err
}

// Cancel satisfies the DataWriter interface
// but is a no-op for the GenericWriter
func (w *GenericWriter) Cancel() {}

// **** Aerospike Backup Writer ****

// ASBEncoder is an interface for encoding the types from the models package into ASB format.
// It extends the Encoder interface.
//
//go:generate mockery --name ASBEncoder
type ASBEncoder interface {
	Encoder
	GetVersionText() []byte
	GetNamespaceMetaText(namespace string) []byte
	GetFirstMetaText() []byte
}

// ASBWriter satisfies the DataWriter interface
// It writes the types from the models package as data encoded in ASB format
// to an io.Writer. It uses an ASBEncoder to encode the data.
type ASBWriter struct {
	GenericWriter
	encoder   ASBEncoder
	namespace string
	first     bool
}

// NewASBWriter creates a new ASBWriter
func NewASBWriter(encoder ASBEncoder, output io.Writer) *ASBWriter {
	return &ASBWriter{
		GenericWriter: *NewGenericWriter(encoder, output),
		encoder:       encoder,
	}
}

// Init initializes the ASBWriter and writes
// the ASB header and metadata to the output.
func (w *ASBWriter) Init(namespace string, first bool) error {
	w.namespace = namespace
	w.first = first

	header := bytes.Buffer{}
	header.Write(w.encoder.GetVersionText())
	header.Write(w.encoder.GetNamespaceMetaText(namespace))
	if first {
		header.Write(w.encoder.GetFirstMetaText())
	}
	_, err := w.output.Write(header.Bytes())

	return err
}

// **** Aerospike Restore Writer ****

// DBWriter is an interface for writing data to an Aerospike cluster.
// The Aerospike Go client satisfies this interface.
//
//go:generate mockery --name DBWriter
type DBWriter interface {
	Put(policy *a.WritePolicy, key *a.Key, bins a.BinMap) a.Error
}

// RestoreWriter satisfies the DataWriter interface
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type RestoreWriter struct {
	asc DBWriter
}

// NewRestoreWriter creates a new RestoreWriter
func NewRestoreWriter(asc DBWriter) *RestoreWriter {
	return &RestoreWriter{
		asc: asc,
	}
}

// Write writes the types from modes to an Aerospike DB.
// While the input type is `any`, the actual types written should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
// TODO support write policy
// TODO support batch writes
func (rw *RestoreWriter) Write(data any) error {
	switch d := data.(type) {
	case *models.Record:
		return rw.asc.Put(nil, d.Key, d.Bins)
	case *models.UDF:
		return rw.writeUDF(d)
	case *models.SIndex:
		return rw.writeSecondaryIndex(d)
	default:
		return nil
	}
}

// writeSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw *RestoreWriter) writeSecondaryIndex(si *models.SIndex) error {

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

	return fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
}

// writeUDF writes a UDF to Aerospike
// TODO check that this does not overwrite existing UDFs
// TODO support write policy
func (rw *RestoreWriter) writeUDF(udf *models.UDF) error {

	// var UDFLang a.Language
	// switch udf.UDFType {
	// case models.LUAUDFType:
	// 	UDFLang = a.LUA
	// default:
	// 	return fmt.Errorf("invalid UDF language: %c", udf.UDFType)
	// }

	// _, err := rw.asc.RegisterUDF(nil, udf.Content, udf.Name, UDFLang)
	// return err

	return fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
}

// Cancel satisfies the DataWriter interface
// but is a no-op for the RestoreWriter
func (rw *RestoreWriter) Cancel() {}
