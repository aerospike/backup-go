package datahandlers

import (
	"backuplib/models"
	"fmt"
	"io"
)

// TODO maybe just accept any in one method and have the encoder check for known types
// to make this more generic and to allow support for subsets of these types
// TODO will probably need a different interface for backup writers
type BackupEncoder interface {
	EncodeMetadata(v models.Metadata) ([]byte, error)
	EncodeRecord(v models.Record) ([]byte, error)
	EncodeUDF(v models.UDF) ([]byte, error)
	EncodeSIndex(v models.SecondaryIndex) ([]byte, error)
}

// BackupWriter satisfies the DataWriter interface
type BackupWriter struct {
	encoder BackupEncoder
	output  io.Writer
}

func NewWriter(encoder BackupEncoder) *BackupWriter {
	return &BackupWriter{
		encoder: encoder,
	}
}

// Write encodes v and writes it to the output
// TODO let the encoder handle the type checking
// TODO maybe restrict the types that can be written to this
func (w *BackupWriter) Write(v interface{}) error {
	var (
		err  error
		data []byte
	)

	switch v := v.(type) {
	case models.Metadata:
		data, err = w.encoder.EncodeMetadata(v)
		if err != nil {
			return err
		}
		_, err = w.output.Write(data)
		return err
	case models.Record:
		data, err = w.encoder.EncodeRecord(v)
		if err != nil {
			return err
		}
		_, err = w.output.Write(data)
		return err
	case models.UDF:
		data, err = w.encoder.EncodeUDF(v)
		if err != nil {
			return err
		}
		_, err = w.output.Write(data)
		return err
	case models.SecondaryIndex:
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
