package datahandlers

import (
	"backuplib/encoder"
	"backuplib/models"
	"bytes"
	"fmt"
	"io"
)

// TODO maybe just accept any in one method and have the encoder check for known types
// to make this more generic and to allow support for subsets of these types
// TODO will probably need a different interface for backup writers
type Encoder interface {
	EncodeRecord(v *models.Record) ([]byte, error)
	EncodeUDF(v *models.UDF) ([]byte, error)
	EncodeSIndex(v *models.SecondaryIndex) ([]byte, error)
}

// GenericWriter satisfies the DataWriter interface
type GenericWriter struct {
	encoder Encoder
	output  io.Writer
}

func NewGenericWriter(encoder Encoder, output io.Writer) *GenericWriter {
	return &GenericWriter{
		encoder: encoder,
		output:  output,
	}
}

// Write encodes v and writes it to the output
// TODO let the encoder handle the type checking
// TODO maybe restrict the types that can be written to this
func (w *GenericWriter) Write(v interface{}) error {
	var (
		err  error
		data []byte
	)

	switch v := v.(type) {
	case *models.Record:
		data, err = w.encoder.EncodeRecord(v)
		if err != nil {
			return err
		}
		_, err = w.output.Write(data)
		return err
	case *models.UDF:
		data, err = w.encoder.EncodeUDF(v)
		if err != nil {
			return err
		}
		_, err = w.output.Write(data)
		return err
	case *models.SecondaryIndex:
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

type ASBWriter struct {
	GenericWriter
	namespace string
	first     bool
}

func NewASBWriter(encoder *encoder.ASBEncoder, output io.Writer) *ASBWriter {
	return &ASBWriter{
		GenericWriter: *NewGenericWriter(encoder, output),
	}
}

func (w *ASBWriter) Init(namespace string, first bool) error {
	w.namespace = namespace
	w.first = first

	header := bytes.Buffer{}
	header.Write(encoder.GetVersionText())
	header.Write(encoder.GetNamespaceMetaText(namespace))
	if first {
		header.Write(encoder.GetFirstMetaText())
	}
	_, err := w.output.Write(header.Bytes())

	return err
}