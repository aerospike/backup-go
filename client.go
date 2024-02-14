package backuplib

import (
	"backuplib/models"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupMarshaller interface {
	MarshalRecord(*a.Record) ([]byte, error)
}

type Config struct{}

type Client struct {
	aerospikeClient *a.Client
	config          Config
}

func NewClient(ac *a.Client, cc Config) (*Client, error) {
	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	return &Client{
		aerospikeClient: ac,
		config:          cc,
	}, nil
}

type Encoder interface {
	EncodeRecord(*models.Record) ([]byte, error)
	EncodeUDF(*models.UDF) ([]byte, error)
	EncodeSIndex(*models.SIndex) ([]byte, error)
}

type EncoderBuilder interface {
	CreateEncoder() (Encoder, error)
	SetDestination(dest io.Writer)
}

// TODO make default constructor for these argument structs
// TODO rename these to options and make this struct contain only optional flags
// required ones should be arguments to the method
type BackupToWriterOptions struct {
	Parallel int
}

func (c *Client) BackupToWriter(writers []io.Writer, enc EncoderBuilder, namespace string, opts BackupToWriterOptions) (*BackupToWriterHandler, <-chan error) {
	args := BackupToWriterOpts{
		BackupOpts: BackupOpts{
			Parallel: opts.Parallel,
		},
	}

	handler := newBackupToWriterHandler(args, c.aerospikeClient, enc, namespace, writers)
	errors := handler.run(writers)

	return handler, errors
}

type Decoder interface {
	NextToken() (any, error)
}

type DecoderBuilder interface {
	CreateDecoder() (Decoder, error)
	SetSource(src io.Reader)
}

type RestoreFromReaderOptions struct {
	Parallel int
}

func (c *Client) RestoreFromReader(readers []io.Reader, dec DecoderBuilder, opts RestoreFromReaderOptions) (*RestoreFromReaderHandler, <-chan error) {
	args := RestoreFromReaderOpts{
		RestoreOpts: RestoreOpts{
			Parallel: opts.Parallel,
		},
	}

	handler := NewRestoreFromReaderHandler(args, c.aerospikeClient, dec, readers)
	errors := handler.Run(readers)

	return handler, errors
}
