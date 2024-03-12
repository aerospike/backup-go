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

package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/models"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Write Worker ****

// dataWriter is an interface for writing data to a destination.
//
//go:generate mockery --name dataWriter
type dataWriter[T any] interface {
	Write(T) error
	Close()
}

// writeWorker implements the pipeline.Worker interface
// It wraps a DataWriter and writes data to it
type writeWorker[T any] struct {
	dataWriter[T]
	receive <-chan T
}

// newWriteWorker creates a new WriteWorker
func newWriteWorker[T any](writer dataWriter[T]) *writeWorker[T] {
	return &writeWorker[T]{
		dataWriter: writer,
	}
}

// SetReceiveChan sets the receive channel for the WriteWorker
func (w *writeWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan satisfies the pipeline.Worker interface
// but is a no-op for the WriteWorker
func (w *writeWorker[T]) SetSendChan(_ chan<- T) {
	// no-op
}

// Run runs the WriteWorker
func (w *writeWorker[T]) Run(ctx context.Context) error {
	defer w.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, active := <-w.receive:
			if !active {
				return nil
			}

			if err := w.Write(data); err != nil {
				return err
			}
		}
	}
}

// **** Token Write Worker ****

type tokenWriteWorker = writeWorker[*models.Token]

// **** Token Writer ****

type tokenWriter = dataWriter[*models.Token]

// statsSetterToken is an interface for setting the stats of a backup job
//
//go:generate mockery --name statsSetterToken --inpackage --exported=false
type statsSetterToken interface {
	addRecords(uint64)
	addUDFs(uint32)
	addSIndexes(uint32)
}

type tokenStatsWriter struct {
	dataWriter[*models.Token]
	stats statsSetterToken
}

func newWriterWithTokenStats(writer dataWriter[*models.Token], stats statsSetterToken) *tokenStatsWriter {
	return &tokenStatsWriter{
		dataWriter: writer,
		stats:      stats,
	}
}

func (tw *tokenStatsWriter) Write(data *models.Token) error {
	err := tw.dataWriter.Write(data)
	if err != nil {
		return err
	}

	switch data.Type {
	case models.TokenTypeRecord:
		tw.stats.addRecords(1)
	case models.TokenTypeUDF:
		tw.stats.addUDFs(1)
	case models.TokenTypeSIndex:
		tw.stats.addSIndexes(1)
	case models.TokenTypeInvalid:
		return errors.New("invalid token")
	}

	return nil
}

// **** Generic Writer ****

// genericWriter satisfies the DataWriter interface
// It writes the types from the models package as encoded data
// to an io.Writer. It uses an Encoder to encode the data.
type genericWriter struct {
	encoder encoding.Encoder
	output  io.Writer
}

// newGenericWriter creates a new GenericWriter
func newGenericWriter(encoder encoding.Encoder, output io.Writer) *genericWriter {
	return &genericWriter{
		encoder: encoder,
		output:  output,
	}
}

// Write encodes v and writes it to the output
// TODO let the encoder handle the type checking
// TODO maybe restrict the types that can be written to this
func (w *genericWriter) Write(v *models.Token) error {
	var (
		err  error
		data []byte
	)

	data, err = w.encoder.EncodeToken(v)
	if err != nil {
		return err
	}

	_, err = w.output.Write(data)

	return err
}

// Cancel satisfies the DataWriter interface
// but is a no-op for the GenericWriter
func (w *genericWriter) Close() {}

// **** Aerospike Backup Writer ****

// asbEncoder is an interface for encoding the types from the models package into ASB format.
// It extends the Encoder interface.
//
//go:generate mockery --name asbEncoder
type asbEncoder interface {
	encoding.Encoder
	GetVersionText() []byte
	GetNamespaceMetaText(namespace string) []byte
	GetFirstMetaText() []byte
}

// asbWriter satisfies the DataWriter interface
// It writes the types from the models package as data encoded in ASB format
// to an io.Writer. It uses an ASBEncoder to encode the data.
type asbWriter struct {
	genericWriter
	encoder   asbEncoder
	once      *sync.Once
	namespace string
	first     bool
}

// newAsbWriter creates a new ASBWriter
func newAsbWriter(encoder asbEncoder, output io.Writer) *asbWriter {
	return &asbWriter{
		genericWriter: *newGenericWriter(encoder, output),
		encoder:       encoder,
		once:          &sync.Once{},
	}
}

// Init initializes the ASBWriter and writes
// the ASB header and metadata to the output.
func (w *asbWriter) Init(namespace string, first bool) error {
	var err error

	w.once.Do(func() {
		w.namespace = namespace
		w.first = first

		header := bytes.Buffer{}
		header.Write(w.encoder.GetVersionText())
		header.Write(w.encoder.GetNamespaceMetaText(namespace))

		if first {
			header.Write(w.encoder.GetFirstMetaText())
		}

		_, err = w.output.Write(header.Bytes())
	})

	return err
}

// **** Aerospike Restore Writer ****

// dbWriter is an interface for writing data to an Aerospike cluster.
// The Aerospike Go client satisfies this interface.
//
//go:generate mockery --name dbWriter
type dbWriter interface {
	Put(policy *a.WritePolicy, key *a.Key, bins a.BinMap) a.Error
}

// restoreWriter satisfies the DataWriter interface
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type restoreWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
}

// newRestoreWriter creates a new RestoreWriter
func newRestoreWriter(asc dbWriter, writePolicy *a.WritePolicy) *restoreWriter {
	return &restoreWriter{
		asc:         asc,
		writePolicy: writePolicy,
	}
}

// Write writes the types from the models package to an Aerospike DB.
// TODO support write policy
// TODO support batch writes
func (rw *restoreWriter) Write(data *models.Token) error {
	switch data.Type {
	case models.TokenTypeRecord:
		return rw.asc.Put(rw.writePolicy, data.Record.Key, data.Record.Bins)
	case models.TokenTypeUDF:
		return rw.writeUDF(data.UDF)
	case models.TokenTypeSIndex:
		return rw.writeSecondaryIndex(data.SIndex)
	case models.TokenTypeInvalid:
		return errors.New("invalid token")
	default:
		return errors.New("unsupported token type")
	}
}

// writeSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw *restoreWriter) writeSecondaryIndex(_ *models.SIndex) error {
	return fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
	//nolint:gocritic // this code will be used to support SIndex backup
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

	//nolint:gocritic // this code will be used to support SIndex backup
	// binName := si.Path.BinName
	// _, err := rw.asc.CreateIndex(nil, si.Namespace, si.Set, si.Name, binName, sindexType)

	// switch {
	// case err.Matches(atypes.INDEX_FOUND):
	// 	//TODO compare sindexes
	// }
} //nolint:wsl // comments need to be kept and should not trigger whitespace warnings

// writeUDF writes a UDF to Aerospike
// TODO check that this does not overwrite existing UDFs
// TODO support write policy
func (rw *restoreWriter) writeUDF(_ *models.UDF) error {
	return fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
	//nolint:gocritic // this code will be used to support UDF backup
	// var UDFLang a.Language
	// switch udf.UDFType {
	// case models.LUAUDFType:
	// 	UDFLang = a.LUA
	// default:
	// 	return fmt.Errorf("invalid UDF language: %c", udf.UDFType)
	// }

	//nolint:gocritic // this code will be used to support UDF backup
	// _, err := rw.asc.RegisterUDF(nil, udf.Content, udf.Name, UDFLang)
	// return err
} //nolint:wsl // comments need to be kept and should not trigger whitespace warnings

// Cancel satisfies the DataWriter interface
// but is a no-op for the RestoreWriter
func (rw *restoreWriter) Close() {}
