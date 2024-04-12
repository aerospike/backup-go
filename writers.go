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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
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

// **** Token Stats Writer ****

// statsSetterToken is an interface for setting the stats of a backup job
//
//go:generate mockery --name statsSetterToken --inpackage --exported=false
type statsSetterToken interface {
	addRecords(uint64)
	addUDFs(uint32)
	addSIndexes(uint32)
}

type tokenStatsWriter struct {
	writer dataWriter[*models.Token]
	stats  statsSetterToken
	logger *slog.Logger
}

func newWriterWithTokenStats(writer dataWriter[*models.Token],
	stats statsSetterToken, logger *slog.Logger) *tokenStatsWriter {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeTokenStats)
	logger.Debug("created new token stats writer")

	return &tokenStatsWriter{
		writer: writer,
		stats:  stats,
		logger: logger,
	}
}

func (tw *tokenStatsWriter) Write(data *models.Token) error {
	err := tw.writer.Write(data)
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

func (tw *tokenStatsWriter) Close() {
	tw.logger.Debug("closed token stats writer")
	tw.writer.Close()
}

// **** Token Writer ****

// tokenWriter satisfies the DataWriter interface
// It writes the types from the models package as encoded data
// to an io.Writer. It uses an Encoder to encode the data.
type tokenWriter struct {
	encoder encoding.Encoder
	output  io.Writer
	logger  *slog.Logger
}

// newTokenWriter creates a new tokenWriter
func newTokenWriter(encoder encoding.Encoder, output io.Writer, logger *slog.Logger) *tokenWriter {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeToken)
	logger.Debug("created new token writer")

	return &tokenWriter{
		encoder: encoder,
		output:  output,
		logger:  logger,
	}
}

// Write encodes v and writes it to the output
func (w *tokenWriter) Write(v *models.Token) error {
	data, err := w.encoder.EncodeToken(v)
	if err != nil {
		return fmt.Errorf("error encoding token: %w", err)
	}

	_, err = w.output.Write(data)

	return err
}

// Cancel satisfies the DataWriter interface
// but is a no-op for the tokenWriter
func (w *tokenWriter) Close() {
	w.logger.Debug("closed token writer")
}

// **** Aerospike Restore Writer ****

// dbWriter is an interface for writing data to an Aerospike cluster.
// The Aerospike Go client satisfies this interface.
//
//go:generate mockery --name dbWriter
type dbWriter interface {
	Put(policy *a.WritePolicy, key *a.Key, bins a.BinMap) a.Error
	CreateComplexIndex(
		policy *a.WritePolicy,
		namespace,
		set,
		indexName,
		binName string,
		indexType a.IndexType,
		indexCollectionType a.IndexCollectionType,
		ctx ...*a.CDTContext,
	) (*a.IndexTask, a.Error)
	DropIndex(policy *a.WritePolicy, namespace, set, indexName string) a.Error
}

// restoreWriter satisfies the DataWriter interface
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type restoreWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	logger      *slog.Logger
}

// newRestoreWriter creates a new RestoreWriter
func newRestoreWriter(asc dbWriter, writePolicy *a.WritePolicy, logger *slog.Logger) *restoreWriter {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeRestore)
	logger.Debug("created new restore writer")

	return &restoreWriter{
		asc:         asc,
		writePolicy: writePolicy,
		logger:      logger,
	}
}

// Write writes the types from the models package to an Aerospike DB.
// TODO support batch writes
func (rw *restoreWriter) Write(data *models.Token) error {
	switch data.Type {
	case models.TokenTypeRecord:
		return rw.writeRecord(&data.Record)
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

func (rw *restoreWriter) writeRecord(record *models.Record) error {
	aerr := rw.asc.Put(rw.writePolicy, record.Key, record.Bins)
	if aerr != nil {
		rw.logger.Error("error writing record", "record", record.Key.Digest(), "error", aerr)
	}

	return aerr
}

// writeSecondaryIndex writes a secondary index to Aerospike
// TODO check that this does not overwrite existing sindexes
// TODO support write policy
func (rw *restoreWriter) writeSecondaryIndex(si *models.SIndex) error {
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
		nil,
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

			err = rw.asc.DropIndex(nil, si.Namespace, si.Set, si.Name)
			if err != nil {
				rw.logger.Error("error dropping sindex", "sindex", si.Name, "error", err)
				return err
			}

			job, err = rw.asc.CreateComplexIndex(
				nil,
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
func (rw *restoreWriter) Close() {
	rw.logger.Debug("closed restore writer")
}
