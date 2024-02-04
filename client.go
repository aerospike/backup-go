package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/handlers"
	"backuplib/models"
	"errors"
	"io"
	"os"

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

type Encoder interface {
	EncodeMetadata(v models.Metadata) ([]byte, error)
	EncodeRecord(v models.Record) ([]byte, error)
	EncodeUDF(v models.UDF) ([]byte, error)
	EncodeSIndex(v models.SecondaryIndex) ([]byte, error)
}

type EncoderFactory interface {
	CreateEncoder() (Encoder, error)
}

// TODO make default constructor for these argument structs
// TODO rename these to options and make this struct contain only optional flags
// required ones should be arguments to the method
type BackupFileArgs struct {
	Namespace  string
	Set        string
	NewEncoder EncoderFactory
	Parallel   int
	FilePath   string
}

func (o *Client) BackupFile(args BackupFileArgs) (*handlers.BackupFileHandler, error) {
	// TODO move this and the pipeline creation into the handler, make handlers for each method
	file, err := os.Create(args.FilePath)
	if err != nil {
		return nil, err
	}
	// TODO this should not close the file, the handler or pipeline needs to close the file
	// the Run method of the handler should be the one that opens and closes the file
	defer file.Close()

	var dst io.Writer = file

	encoder, err := args.NewEncoder.CreateEncoder()
	if err != nil {
		return nil, err
	}

	readers := make([]datahandlers.DataReader, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		var first bool
		if i == 0 {
			first = true
		}

		begin := (i * PARTITIONS) / args.Parallel
		count := PARTITIONS / args.Parallel // TODO verify no off by 1 error

		ARCFG := &datahandlers.ARConfig{
			Namespace:      args.Namespace,
			Set:            args.Set,
			FirstPartition: begin,
			NumPartitions:  count,
			First:          first,
		}

		dataReader, err := datahandlers.NewAerospikeReader(
			ARCFG,
			o.aerospikeClient,
		)
		if err != nil {
			return nil, err
		}

		readers[i] = dataReader
	}

	processors := make([]datahandlers.DataProcessor, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	dataWriter := datahandlers.NewGenericWriter(encoder, dst)

	writers := []datahandlers.DataWriter{dataWriter}

	pipeline := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	BFArgs := handlers.BackupFileArgs{
		BackupArgs: handlers.BackupArgs{
			Mode: handlers.SingleFile,
		},
		FilePath: args.FilePath,
	}

	handler, err := handlers.NewBackupFileHandler(pipeline, BFArgs)
	if err != nil {
		return nil, err
	}

	err = handler.Run()
	return handler, err
}

func (c *Client) BackupToWriter() (*handlers.BackupToWriterHandler, error) {
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
