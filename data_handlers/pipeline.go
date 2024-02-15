package datahandlers

import (
	"context"
	"io"
	"math"
	"sync"
)

// Reader is an interface for reading data
//
//go:generate mockery --name Reader
type Reader interface {
	// Read must return io.EOF when there is no more data to read
	Read() (any, error)
	// Cancel tells the reader to clean up its resources
	// usually this is a no-op
	Cancel()
}

// Writer is an interface for writing data
//
//go:generate mockery --name Writer
type Writer interface {
	Write(any) error
	// Cancel tells the writer to clean up its resources
	// usually this is a no-op
	Cancel()
}

// Processor is an interface for processing data
//
//go:generate mockery --name Processor
type Processor interface {
	Process(any) (any, error)
}

type DataPipeline struct {
	readers         []readStage
	processors      []processStage
	writers         []writeStage
	readSendChan    chan any
	processSendChan chan any
}

func NewDataPipeline(r []Reader, p []Processor, w []Writer) *DataPipeline {
	if len(r) == 0 || len(p) == 0 || len(w) == 0 {
		return nil
	}

	chanSize := int(math.Max(math.Max(float64(len(r)), float64(len(p))), float64(len(w))))

	readSendChan := make(chan any, chanSize)
	readers := make([]readStage, len(r))
	for i, reader := range r {
		readers[i] = readStage{
			r:    reader,
			send: readSendChan,
		}
	}

	processSendChan := make(chan any, chanSize)
	processors := make([]processStage, len(p))
	for i, processor := range p {
		processors[i] = processStage{
			p:       processor,
			receive: readSendChan,
			send:    processSendChan,
		}
	}

	writers := make([]writeStage, len(w))
	for i, writer := range w {
		writers[i] = writeStage{
			w:       writer,
			receive: processSendChan,
		}
	}

	return &DataPipeline{
		readers:         readers,
		processors:      processors,
		writers:         writers,
		readSendChan:    readSendChan,
		processSendChan: processSendChan,
	}
}

func (dp *DataPipeline) Run() error {
	chanLen := len(dp.readers) + len(dp.processors) + len(dp.writers)

	if chanLen == 0 {
		return nil
	}

	errc := make(chan error, chanLen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rg := &sync.WaitGroup{}
	for _, reader := range dp.readers {
		rg.Add(1)
		go func(reader readStage) {
			defer rg.Done()
			err := reader.Run(ctx)
			if err != nil {
				cancel()
				errc <- err
			}
		}(reader)
	}

	pg := &sync.WaitGroup{}
	for _, processor := range dp.processors {
		pg.Add(1)
		go func(processor processStage) {
			defer pg.Done()
			err := processor.Run(ctx)
			if err != nil {
				cancel()
				errc <- err
			}
		}(processor)
	}

	wg := &sync.WaitGroup{}
	for _, writer := range dp.writers {
		wg.Add(1)
		go func(writer writeStage) {
			defer wg.Done()
			err := writer.Run(ctx)
			if err != nil {
				cancel()
				errc <- err
			}
		}(writer)
	}

	rg.Wait()
	close(dp.readSendChan)
	pg.Wait()
	close(dp.processSendChan)
	wg.Wait()
	close(errc)

	// TODO improve error handling
	// this should return a slice of errors
	// each error should identify the stage it came from
	return <-errc
}

type readStage struct {
	r    Reader
	send chan any
}

// TODO support passing in a context
func (rs *readStage) Run(ctx context.Context) error {
	defer rs.r.Cancel()
	for {
		v, err := rs.r.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case rs.send <- v:
		}
	}
}

type processStage struct {
	p       Processor
	send    chan any
	receive chan any
}

func (ps *processStage) Run(ctx context.Context) error {
	for {
		var (
			v      any
			active bool
		)
		select {
		case <-ctx.Done():
			return nil
		case v, active = <-ps.receive:
		}
		if !active {
			return nil
		}
		v, err := ps.p.Process(v)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case ps.send <- v:
		}
	}
}

type writeStage struct {
	w       Writer
	receive chan any
}

func (ws *writeStage) Run(ctx context.Context) error {
	defer ws.w.Cancel()
	for {
		var (
			v      any
			active bool
		)
		select {
		case <-ctx.Done():
			return nil
		case v, active = <-ws.receive:
		}
		if !active {
			return nil
		}
		err := ws.w.Write(v)
		if err != nil {
			return err
		}
	}
}
