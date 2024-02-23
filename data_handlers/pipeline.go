// Copyright 2024-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
type Reader[T any] interface {
	// Read must return io.EOF when there is no more data to read
	Read() (T, error)
	// Cancel tells the reader to clean up its resources
	// usually this is a no-op
	Cancel()
}

// Writer is an interface for writing data
//
//go:generate mockery --name Writer
type Writer[T any] interface {
	Write(T) error
	// Cancel tells the writer to clean up its resources
	// usually this is a no-op
	Cancel()
}

// Processor is an interface for processing data
//
//go:generate mockery --name Processor
type Processor[T any] interface {
	Process(T) (T, error)
}

// DataPipeline is a generic pipeline for reading, processing, and writing data.
// Pipelines are created with slices of Readers, Processors, and Writers.
// The Run method starts the pipeline.
// Each reader, writer, and processor runs in its own goroutine.
// Each type of stage (read, process, write) is connected by a single channel.
type DataPipeline[T any] struct {
	readers         []readStage[T]
	processors      []processStage[T]
	writers         []writeStage[T]
	readSendChan    chan T
	processSendChan chan T
}

// NewDataPipeline creates a new DataPipeline
func NewDataPipeline[T any](r []Reader[T], p []Processor[T], w []Writer[T]) *DataPipeline[T] {
	if len(r) == 0 || len(p) == 0 || len(w) == 0 {
		return nil
	}

	chanSize := int(math.Max(math.Max(float64(len(r)), float64(len(p))), float64(len(w))))

	readSendChan := make(chan T, chanSize)
	readers := make([]readStage[T], len(r))
	for i, reader := range r {
		readers[i] = readStage[T]{
			r:    reader,
			send: readSendChan,
		}
	}

	processSendChan := make(chan T, chanSize)
	processors := make([]processStage[T], len(p))
	for i, processor := range p {
		processors[i] = processStage[T]{
			p:       processor,
			receive: readSendChan,
			send:    processSendChan,
		}
	}

	writers := make([]writeStage[T], len(w))
	for i, writer := range w {
		writers[i] = writeStage[T]{
			w:       writer,
			receive: processSendChan,
		}
	}

	return &DataPipeline[T]{
		readers:         readers,
		processors:      processors,
		writers:         writers,
		readSendChan:    readSendChan,
		processSendChan: processSendChan,
	}
}

// Run starts the pipeline
// The pipeline stops when all readers, processors, and writers finish or
// if any stage returns an error.
func (dp *DataPipeline[T]) Run() error {
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
		go func(reader readStage[T]) {
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
		go func(processor processStage[T]) {
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
		go func(writer writeStage[T]) {
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

type readStage[T any] struct {
	r    Reader[T]
	send chan T
}

// TODO support passing in a context
func (rs *readStage[T]) Run(ctx context.Context) error {
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

type processStage[T any] struct {
	p       Processor[T]
	send    chan T
	receive chan T
}

func (ps *processStage[T]) Run(ctx context.Context) error {
	for {
		var (
			v      T
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

type writeStage[T any] struct {
	w       Writer[T]
	receive chan T
}

func (ws *writeStage[T]) Run(ctx context.Context) error {
	defer ws.w.Cancel()
	for {
		var (
			v      T
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
