package workers

import (
	"errors"
	"io"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type RestoreUnmarshaller interface {
	NextToken() (any, error)
}

type InputOpener interface {
	Open() (io.ReadCloser, error)
}

type RestoreConfig struct {
	Namespace    string
	Set          string
	Input        InputOpener
	UnMarshaller RestoreUnmarshaller
	First        bool
}

type RestoreStatus struct{}

type RestoreWorker struct {
	config       RestoreConfig
	client       *a.Client
	workLock     *sync.Mutex
	inputOpener  InputOpener
	UnMarshaller RestoreUnmarshaller
	reader       io.ReadCloser
}

func NewRestoreWorker(cfg RestoreConfig, ac *a.Client, ip InputOpener, um RestoreUnmarshaller) (*RestoreWorker, error) {
	rc, err := ip.Open()
	if err != nil {
		return nil, err
	}

	return &RestoreWorker{
		config:       cfg,
		client:       ac,
		workLock:     &sync.Mutex{},
		inputOpener:  ip,
		UnMarshaller: um,
		reader:       rc,
	}, nil
}

func (rw *RestoreWorker) lock() {
	rw.workLock.Lock()
}

func (rw *RestoreWorker) unlock() {
	rw.workLock.Unlock()
}

func (rw *RestoreWorker) Run() error {
	rw.lock()
	defer rw.unlock()

	return errors.New("UNIMPLEMENTED")
}

func (rw *RestoreWorker) Resume() error {
	return errors.New("UNIMPLEMENTED")
}
