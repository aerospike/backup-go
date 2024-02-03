package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/handlers"
	"errors"
	"io"
	"os"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	PARTITIONS = 4096
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
	NextToken() (any, error)
}

type EncoderFactory interface {
	CreateEncoder() (Encoder, error)
}

// TODO make default constructor for these argument structs
type BackupFileArgs struct {
	Namespace  string
	Set        string
	NewEncoder EncoderFactory
	Parallel   int
	FilePath   string
}

// TODO finish converting this from restore logic to backup logic
func (o *Client) BackupFile(args BackupFileArgs) (*handlers.BackupHandler, error) {
	file, err := os.Open(args.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var src io.Reader = file

	encoder, err := args.NewEncoder.CreateEncoder(src)
	if err != nil {
		return nil, err
	}

	reader := datahandlers.NewGenericReader(decoder)
	readers := []datahandlers.DataReader{reader}

	processors := make([]datahandlers.DataProcessor, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]datahandlers.DataWriter, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		writer := datahandlers.NewRestoreWriter(o.aerospikeClient)
		writers[i] = writer
	}

	pipeline := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	restoreArgs := handlers.RestoreArgs{}
	handler, err := handlers.NewRestoreHandler(pipeline, restoreArgs)
	if err != nil {
		return nil, err
	}

	err = handler.Run()
	return handler, err
}

func (o *Client) ResumeBackup(bh *handlers.BackupHandler) (*handlers.BackupHandler, error) {
	return nil, errors.New("UNIMPLEMENTED")
}

type RestoreDirectoryArgs struct {
	// TODO
}

func (o *Client) RestoreDirectory(args *RestoreDirectoryArgs) (*handlers.RestoreHandler, error) {
	return nil, errors.New("UNIMPLEMENTED")
}

type Decoder interface {
	NextToken() (any, error)
}

type DecoderFactory interface {
	CreateDecoder(src io.Reader) (Decoder, error)
}

type RestoreFileArgs struct {
	NewDecoder DecoderFactory // TODO the decoders need to take an opener closer
	FilePath   string
	Parallel   int
}

func (o *Client) RestoreFile(args *RestoreFileArgs) (*handlers.RestoreHandler, error) {
	file, err := os.Open(args.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var src io.Reader = file

	decoder, err := args.NewDecoder.CreateDecoder(src)
	if err != nil {
		return nil, err
	}

	reader := datahandlers.NewGenericReader(decoder)
	readers := []datahandlers.DataReader{reader}

	processors := make([]datahandlers.DataProcessor, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]datahandlers.DataWriter, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		writer := datahandlers.NewRestoreWriter(o.aerospikeClient)
		writers[i] = writer
	}

	pipeline := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	restoreArgs := handlers.RestoreArgs{}
	handler, err := handlers.NewRestoreHandler(pipeline, restoreArgs)
	if err != nil {
		return nil, err
	}

	err = handler.Run()
	return handler, err
}
