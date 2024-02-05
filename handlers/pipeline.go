package handlers

import (
	"io"
	"math"
	"sync"
)

// DataReader is an interface for reading data
type DataReader interface {
	// Read must return io.EOF when there is no more data to read
	Read() (any, error)
}

type DataWriter interface {
	Write(any) error
}

type DataProcessor interface {
	Process(any) (any, error)
}

type DataPipeline struct {
	runLock         *sync.Mutex
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
		runLock:         &sync.Mutex{},
		readSendChan:    readSendChan,
		processSendChan: processSendChan,
	}
}

// TODO support passing in a context
func (dp *DataPipeline) run() error {
	dp.runLock.Lock()
	defer dp.runLock.Unlock()

	errc := make(chan error, len(dp.readers)+len(dp.processors)+len(dp.writers))

	rg := &sync.WaitGroup{}
	for _, reader := range dp.readers {
		rg.Add(1)
		go func(reader readStage) {
			defer rg.Done()
			err := reader.Run()
			if err != nil {
				errc <- err
			}
		}(reader)
	}

	pg := &sync.WaitGroup{}
	for _, processor := range dp.processors {
		pg.Add(1)
		go func(processor processStage) {
			defer pg.Done()
			err := processor.Run()
			if err != nil {
				errc <- err
			}
		}(processor)
	}

	wg := &sync.WaitGroup{}
	for _, writer := range dp.writers {
		wg.Add(1)
		go func(writer writeStage) {
			defer wg.Done()
			err := writer.Run()
			if err != nil {
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

	return <-errc
}

func (dp *DataPipeline) Wait() {
	dp.runLock.Lock()
	defer dp.runLock.Unlock()
}

type readStage struct {
	r    DataReader
	send chan any
}

// TODO support passing in a context
func (rs *readStage) Run() error {
	for {
		v, err := rs.r.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rs.send <- v
	}
}

type processStage struct {
	p       DataProcessor
	send    chan any
	receive chan any
}

// TODO support passing in a context
func (ps *processStage) Run() error {
	for {
		v, active := <-ps.receive
		if !active {
			return nil
		}
		v, err := ps.p.Process(v)
		if err != nil {
			return err
		}
		ps.send <- v
	}
}

type writeStage struct {
	w       DataWriter
	receive chan any
}

func (ws *writeStage) Run() error {
	for {
		v, active := <-ws.receive
		if !active {
			return nil
		}
		err := ws.w.Write(v)
		if err != nil {
			return err
		}
	}
}
