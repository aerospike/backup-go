// Copyright 2024 Aerospike, Inc.
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

package backup

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

// State contains current backups status data.
type State struct {
	// Global backup context.
	ctx context.Context

	// Counter to count how many times State instance was initialized.
	// Is used to create suffix for backup files.
	Counter int
	// RecordsStateChan communication channel to save current filter state.
	RecordsStateChan chan models.PartitionFilterSerialized
	// RecordStates store states of all filters.
	RecordStates map[int]models.PartitionFilterSerialized

	RecordStatesSaved map[int]models.PartitionFilterSerialized
	// SaveCommandChan command to save current state for worker.
	SaveCommandChan chan int
	// Mutex for RecordStates operations.
	// Ordinary mutex is used, because we must not allow any writings when we read state.
	mu sync.Mutex
	// File to save state to.
	FileName string

	// writer is used to create a state file.
	writer Writer
	// logger for logging errors.
	logger *slog.Logger
}

// NewState returns new state instance depending on config.
// If we continue back up, the state will be loaded from a state file,
// if it is the first operation, new state instance will be returned.
func NewState(
	ctx context.Context,
	config *BackupConfig,
	reader StreamingReader,
	writer Writer,
	logger *slog.Logger,
) (*State, error) {
	switch {
	case config.isStateFirstRun():
		logger.Debug("initializing new state")
		return newState(ctx, config, writer, logger), nil
	case config.isStateContinue():
		logger.Debug("initializing state from file", slog.String("file", config.StateFile))
		return newStateFromFile(ctx, config, reader, writer, logger)
	}

	return nil, nil
}

// newState creates status service from parameters, for backup operations.
func newState(
	ctx context.Context,
	config *BackupConfig,
	writer Writer,
	logger *slog.Logger,
) *State {
	s := &State{
		ctx: ctx,
		// RecordsStateChan must not be buffered, so we can stop all operations.
		RecordsStateChan:  make(chan models.PartitionFilterSerialized),
		RecordStates:      make(map[int]models.PartitionFilterSerialized),
		RecordStatesSaved: make(map[int]models.PartitionFilterSerialized),
		SaveCommandChan:   make(chan int),
		FileName:          config.StateFile,
		writer:            writer,
		logger:            logger,
	}
	// Run watcher on initialization.
	go s.serve()
	go s.serveRecords()

	return s
}

// newStateFromFile creates a status service from the file, to continue operations.
func newStateFromFile(
	ctx context.Context,
	config *BackupConfig,
	reader StreamingReader,
	writer Writer,
	logger *slog.Logger,
) (*State, error) {
	f, err := openFile(ctx, reader, config.StateFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}

	dec := gob.NewDecoder(f)

	var s State
	if err = dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}

	s.ctx = ctx
	s.writer = writer
	s.logger = logger
	s.RecordsStateChan = make(chan models.PartitionFilterSerialized)
	s.SaveCommandChan = make(chan int)
	s.Counter++

	// Init current state.
	for k, v := range s.RecordStatesSaved {
		s.RecordStates[k] = v
	}

	logger.Debug("loaded state file successfully", slog.Int("filters loaded", len(s.RecordStatesSaved)))

	// Run watcher on initialization.
	go s.serve()
	go s.serveRecords()

	return &s, nil
}

// serve dumps files to disk.
func (s *State) serve() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.SaveCommandChan:
			if err := s.dump(msg); err != nil {
				s.logger.Error("failed to dump state", slog.Any("error", err))
				return
			}
		}
	}
}

func (s *State) dump(n int) error {
	file, err := s.writer.NewWriter(s.ctx, s.FileName)
	if err != nil {
		return fmt.Errorf("failed to create state file %s: %w", s.FileName, err)
	}

	enc := gob.NewEncoder(file)

	s.mu.Lock()
	defer s.mu.Unlock()

	if n > -1 {
		s.RecordStatesSaved[n] = s.RecordStates[n]
	}

	if err = enc.Encode(s); err != nil {
		return fmt.Errorf("failed to encode state data: %w", err)
	}

	if err = file.Close(); err != nil {
		return fmt.Errorf("failed to close state file: %w", err)
	}

	s.logger.Debug("state file dumped", slog.Time("saved at", time.Now()))

	return nil
}

func (s *State) initState(pf []*a.PartitionFilter) error {
	s.mu.Lock()

	for i := range pf {
		pfs, err := models.NewPartitionFilterSerialized(pf[i])
		if err != nil {
			return err
		}

		s.RecordStates[i] = pfs
		s.RecordStatesSaved[i] = pfs
	}
	// Do not move this Unlock() to defer!
	s.mu.Unlock()

	return s.dump(-1)
}

func (s *State) loadPartitionFilters() ([]*a.PartitionFilter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*a.PartitionFilter, 0, len(s.RecordStatesSaved))

	for _, state := range s.RecordStatesSaved {
		f, err := state.Decode()
		if err != nil {
			return nil, err
		}

		result = append(result, f)
	}

	return result, nil
}

func (s *State) serveRecords() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case state := <-s.RecordsStateChan:
			if state.IsEmpty() {
				continue
			}

			s.mu.Lock()
			s.RecordStates[state.N] = state
			s.mu.Unlock()
		}
	}
}

func (s *State) getFileSuffix() string {
	if s.Counter > 0 {
		return fmt.Sprintf("(%d)", s.Counter)
	}

	return ""
}

func openFile(ctx context.Context, reader StreamingReader, fileName string) (io.ReadCloser, error) {
	readCh := make(chan models.File)
	errCh := make(chan error)

	go reader.StreamFile(ctx, fileName, readCh, errCh)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case file := <-readCh:
		return file.Reader, nil
	}
}
