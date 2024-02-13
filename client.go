package backuplib

import (
	"backuplib/handlers"
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

// TODO IMPORTANT: implement Backup and Restore io.Reader and io.Writer methods
// these can serve as the basis for the file methods and all other methods
// under this paradigm, the file methods would only need to handle file opening and closing
// pipelines should be created at generic/io.reader/writer backup and read Run time so that the file methods
// just repetadly call the Run method of the generic handler with new io.readers/writers

type EncoderBuilder interface {
	CreateEncoder() (handlers.Encoder, error)
	SetDestination(dest io.Writer)
}

// TODO make default constructor for these argument structs
// TODO rename these to options and make this struct contain only optional flags
// required ones should be arguments to the method
type BackupToWriterOptions struct {
	Parallel int
}

func (c *Client) BackupToWriter(writers []io.Writer, enc EncoderBuilder, namespace string, opts BackupToWriterOptions) (*handlers.BackupToWriterHandler, <-chan error) {
	args := handlers.BackupToWriterOpts{
		BackupOpts: handlers.BackupOpts{
			Parallel: opts.Parallel,
		},
	}

	handler := handlers.NewBackupToWriterHandler(args, c.aerospikeClient, enc, namespace, writers)
	errors := handler.Run(writers)

	return handler, errors
}

type DecoderBuilder interface {
	CreateDecoder() (handlers.Decoder, error)
	SetSource(src io.Reader)
}

type RestoreFromReaderOptions struct {
	Parallel int
}

func (c *Client) RestoreFromReader(readers []io.Reader, dec DecoderBuilder, opts RestoreFromReaderOptions) (*handlers.RestoreFromReaderHandler, <-chan error) {
	args := handlers.RestoreFromReaderArgs{
		RestoreArgs: handlers.RestoreArgs{
			Parallel: opts.Parallel,
		},
	}

	handler := handlers.NewRestoreFromReaderHandler(args, c.aerospikeClient, dec, readers)
	errors := handler.Run(readers)

	return handler, errors
}
