package handlers

import (
	"backuplib/output"
	"backuplib/workers"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type WorkMode int

const (
	Invalid WorkMode = iota
	SingleFile
	Directory
	// s3File
	// s3Dir
)

type BackupMarshaller interface {
	MarshalRecord(*a.Record) ([]byte, error)
}

type BackupArgs struct {
	Namespace  string
	Set        string
	Marshaller BackupMarshaller
	Parallel   int
	FilePath   string
	DirPath    string
	// TODO S3Path
}

type outputHandler interface {
	Run(ctx context.Context) error
	Resume(ctx context.Context) error
	// TODO a get stats method
}

func newOutPutHandler(mode WorkMode, numWorkers int, args BackupArgs) (outputHandler, error) {
	switch mode {
	case Directory:
		directory := args.DirPath
		filePrefix := args.Namespace
		return output.NewDirectoryHandler(
			directory,
			filePrefix,
			numWorkers,
		), nil
	default:
		return nil, fmt.Errorf("unknown backup output mode %v", mode)
	}
}

type BackupHandlerStatus struct {
	active      bool
	recordCount int
	outputMode  WorkMode
}

// this is the public API
// TODO use the WorkHandler to handle running jobs
type BackupHandler struct {
	jobs          []*workers.BackupJob
	status        BackupHandlerStatus
	args          BackupArgs
	outputHandler outputHandler
	errors        chan error
	wg            *sync.WaitGroup
	workLock      *sync.Mutex
}

func NewBackupHandler(jobs []*workers.BackupJob, args BackupArgs, outputMode WorkMode) (*BackupHandler, error) {
	outputHandler, err := newOutPutHandler(
		outputMode,
		len(jobs),
		args,
	)
	if err != nil {
		return nil, err
	}

	bh := &BackupHandler{
		jobs: jobs,
		status: BackupHandlerStatus{
			outputMode: outputMode,
		},
		outputHandler: outputHandler,
		args:          args,
		errors:        make(chan error, len(jobs)),
		wg:            &sync.WaitGroup{},
		workLock:      &sync.Mutex{},
	}

	return bh, nil
}

func (o *BackupHandler) lock() {
	o.workLock.Lock()
}

func (o *BackupHandler) unlock() {
	o.workLock.Unlock()
}

func (o *BackupHandler) Run() error {
	o.lock()
	// TODO i should start at 1 if oneshot is done before this
	// TODO maybe these should use a context that cancels all jobs if one fails
	// TODO make sure the single shot work gets done first
	// there are a few cases here
	// all backup jobs finish
	// one or more error
	go func() {
		defer o.unlock()
		// TODO allow passing in a context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for _, job := range o.jobs {
			o.wg.Add(1)
			go func(j *workers.BackupJob, ctx context.Context) {
				defer o.wg.Done()

				select {
				case <-ctx.Done():
					return
				default:
					err := j.Run()
					if err != nil {
						log.Println(err) // TODO error logging shouldn't be here
						o.errors <- err
						cancel()
					}
				}
			}(job, ctx)
		}

		o.wg.Wait()
		close(o.errors)
	}()

	fmt.Println("backup started")

	return nil
}

func (o *BackupHandler) Resume() error {
	return o.Run()
}

func (o *BackupHandler) Wait() {
	o.wg.Wait()
}

func (o *BackupHandler) GetStats() (BackupHandlerStatus, error) {
	return BackupHandlerStatus{}, errors.New("UNIMPLEMENTED")
}
