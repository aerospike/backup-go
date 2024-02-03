package datahandlers

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

// TODO maybe the steps in the pipeline should exchange information on channels to make
// them thread safe
type DataPipeline struct {
	runLock    *sync.Mutex
	readers    []readStage
	processors []processStage
	writers    []writeStage
}

func NewDataPipeline(r []DataReader, p []DataProcessor, w []DataWriter) *DataPipeline {

	chanSize := int(math.Max(math.Max(float64(len(r)), float64(len(p))), float64(len(w))))
	readSendChan := make(chan any, chanSize)

	processRecieveChan := readSendChan
	processSendChan := make(chan any, chanSize)

	writeRecieveChan := processSendChan

	readers := make([]readStage, len(r))
	for i, reader := range r {
		readers[i] = readStage{
			r:    reader,
			send: readSendChan,
		}
	}

	processors := make([]processStage, len(p))
	for i, processor := range p {
		processors[i] = processStage{
			p:       processor,
			receive: processRecieveChan,
			send:    processSendChan,
		}
	}

	writers := make([]writeStage, len(w))
	for i, writer := range w {
		writers[i] = writeStage{
			w:       writer,
			receive: writeRecieveChan,
		}
	}

	return &DataPipeline{
		readers:    readers,
		processors: processors,
		writers:    writers,
		runLock:    &sync.Mutex{},
	}
}

// TODO support passing in a context
// TODO support a parallel flag to bound the number of concurrent stages
func (dp *DataPipeline) Run() error {
	dp.runLock.Lock()
	defer dp.runLock.Unlock()

	errc := make(chan error)
	wg := &sync.WaitGroup{}

	for _, reader := range dp.readers {
		wg.Add(1)
		go func(reader readStage) {
			defer wg.Done()
			err := reader.Run()
			if err != nil {
				errc <- err
			}
		}(reader)
	}

	for _, processor := range dp.processors {
		wg.Add(1)
		go func(processor processStage) {
			defer wg.Done()
			err := processor.Run()
			if err != nil {
				errc <- err
			}
		}(processor)
	}

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

	close(errc)

	err := <-errc
	if err != nil {
		return err
	}

	wg.Wait()

	return nil
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
			close(rs.send)
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
			close(ps.send)
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
