package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/handlers"
	"backuplib/output"
	"backuplib/workers"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	PARTITIONS = 4096
)

type BackupMarshaller interface {
	MarshalRecord(*a.Record) ([]byte, error)
}

type Config struct {
	Host a.Host
}

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

type BackupDirectoryArgs struct {
	Namespace  string
	Set        string
	Marshaller BackupMarshaller
	Parallel   int
	DirPath    string
}

func (o *Client) BackupDirectory(c BackupDirectoryArgs) (*handlers.BackupHandler, error) {
	err := os.Mkdir(c.DirPath, 0755)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	backupWorkers := make([]*workers.BackupJob, c.Parallel)
	for i := 0; i < c.Parallel; i++ {
		begin := (i * PARTITIONS) / c.Parallel
		count := PARTITIONS / c.Parallel // TODO verify no off by 1 error

		// TODO check directory for existing backup files
		// error if they are found unless -r is used
		fileName := fmt.Sprintf("%s_%05d.asb", c.Namespace, i)
		filePath := filepath.Join(c.DirPath, fileName)
		// TODO this FD only needs to be opened in write mode
		// create opens in RDWR
		writer, err := os.Create(filePath)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		writer.Close()

		out := output.NewFile(filePath)

		backupJobConfig := &workers.BackupJobConfig{
			Namespace:      c.Namespace,
			Set:            c.Set,
			Output:         out,
			Marshaller:     c.Marshaller,
			FirstPartition: begin,
			NumPartitions:  count,
		}

		if i == 0 {
			backupJobConfig.First = true
		}

		worker, err := workers.NewBackupJob(
			backupJobConfig,
			o.aerospikeClient,
		)
		if err != nil {
			return nil, err
		}

		backupWorkers[i] = worker
	}

	backupArgs := handlers.BackupArgs{
		Namespace:  c.Namespace,
		Set:        c.Set,
		Marshaller: c.Marshaller,
		Parallel:   c.Parallel,
		DirPath:    c.DirPath,
	}

	backupHandler, err := handlers.NewBackupHandler(
		backupWorkers,
		backupArgs,
		handlers.Directory,
	)
	if err != nil {
		return nil, err
	}

	err = backupHandler.Run()
	return backupHandler, err
}

type BackupFileArgs struct {
	Namespace  string
	Set        string
	Marshaller BackupMarshaller
	Parallel   int
	FilePath   string
}

func (o *Client) BackupFile(args BackupFileArgs) (*handlers.BackupHandler, error) {
	writer, err := os.Create(args.FilePath)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	writer.Close()
	out := output.NewLockedFile(args.FilePath)

	backupWorkers := make([]*workers.BackupJob, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		begin := (i * PARTITIONS) / args.Parallel
		count := PARTITIONS / args.Parallel // TODO verify no off by 1 error

		backupJobConfig := &workers.BackupJobConfig{
			Namespace:      args.Namespace,
			Set:            args.Set,
			Output:         out,
			Marshaller:     args.Marshaller,
			FirstPartition: begin,
			NumPartitions:  count,
		}

		if i == 0 {
			backupJobConfig.First = true
		}

		worker, err := workers.NewBackupJob(
			backupJobConfig,
			o.aerospikeClient,
		)
		if err != nil {
			return nil, err
		}

		backupWorkers[i] = worker
	}

	backupArgs := handlers.BackupArgs{
		Namespace:  args.Namespace,
		Set:        args.Set,
		Marshaller: args.Marshaller,
		Parallel:   args.Parallel,
		FilePath:   args.FilePath,
	}

	backupHandler, err := handlers.NewBackupHandler(
		backupWorkers,
		backupArgs,
		handlers.SingleFile,
	)
	if err != nil {
		return nil, err
	}

	err = backupHandler.Run()
	return backupHandler, err
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

type NewDecoder func(src io.Reader) Decoder

type RestoreFileArgs struct {
	NewDecoder NewDecoder // TODO the decoders need to take an opener closer
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
	// the readers will be reading from the same file
	// if there are multiple workers so we need to lock the file
	if args.Parallel > 1 {
		src = NewLockedReader(file)
	}

	decoder := args.NewDecoder(src)

	pf := datahandlers.NewDataPipelineFactory(
		datahandlers.NewGenericReaderFactory(decoder),
		datahandlers.NewNOOPProcessorFactory(),
		datahandlers.NewRestoreWriterFactory(o.aerospikeClient),
	)

	jobs := make([]*datahandlers.DataPipeline, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		jobs[i] = pf.CreatePipeline()
	}

	restoreArgs := handlers.RestoreArgs{
		Parallel: args.Parallel,
	}

	return handlers.NewRestoreHandler(jobs, restoreArgs)
}
