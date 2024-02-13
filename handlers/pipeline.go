package handlers

import (
	"context"
	"io"
	"math"
	"sync"
)

// DataReader is an interface for reading data
type DataReader interface {
	// Read must return io.EOF when there is no more data to read
	Read() (any, error)
	// Cancel tells the reader to clean up its resources
	// usually this is a no-op
	Cancel() error
}

type DataWriter interface {
	Write(any) error
	// Cancel tells the writer to clean up its resources
	// usually this is a no-op
	Cancel() error
}

type DataProcessor interface {
	Process(any) (any, error)
}

type DataPipeline struct {
	readers         []readStage
	processors      []processStage
	writers         []writeStage
	readSendChan    chan any
	processSendChan chan any
}

func NewDataPipeline(r []DataReader, p []DataProcessor, w []DataWriter) *DataPipeline {

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

func (dp *DataPipeline) run() error {
	errc := make(chan error, len(dp.readers)+len(dp.processors)+len(dp.writers))

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
	r    DataReader
	send chan any
}

// TODO support passing in a context
func (rs *readStage) Run(ctx context.Context) error {
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
	p       DataProcessor
	send    chan any
	receive chan any
}

// TODO support passing in a context
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
	w       DataWriter
	receive chan any
}

func (ws *writeStage) Run(ctx context.Context) error {
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
