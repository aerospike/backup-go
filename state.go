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
	"path/filepath"
	"sync"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

// State contains current backups status data.
// Fields must be Exportable for marshaling to GOB.
type State struct {
	// Global backup context.
	ctx context.Context

	// Counter tracks the number of times the State instance has been initialized.
	// This is used to generate a unique suffix for backup files.
	Counter int
	// RecordsStateChan is a channel for communicating serialized partition filter
	// states.
	RecordsStateChan chan models.PartitionFilterSerialized
	// RecordStates stores the current states of all partition filters.
	RecordStates map[int]models.PartitionFilterSerialized

	RecordStatesSaved map[int]models.PartitionFilterSerialized
	// SaveCommandChan command to save current state for worker.
	SaveCommandChan chan int
	// Mutex for synchronizing operations on record states.
	mu sync.Mutex
	// FileName specifies the file name where the backup state is persisted.
	FileName string
	// FilePath specifies the file path where the backup state is persisted.
	FilePath string

	// writer is used to create a state file.
	writer Writer
	// logger for logging errors.
	logger *slog.Logger
}

// NewState creates and returns a State instance. If continuing a previous
// backup, the state is loaded from the specified state file. Otherwise, a
// new State instance is created.
func NewState(
	ctx context.Context,
	config *ConfigBackup,
	reader StreamingReader,
	writer Writer,
	logger *slog.Logger,
) (*State, error) {
	logger.Debug("Initializing state", slog.String("path", config.StateFile))

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

// newState creates a new State instance for backup operations.
func newState(
	ctx context.Context,
	config *ConfigBackup,
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
		FilePath:          config.StateFile,
		FileName:          filepath.Base(config.StateFile),
		writer:            writer,
		logger:            logger,
	}

	// Run watcher on initialization.
	go s.serve()
	go s.serveRecords()

	return s
}

// newStateFromFile creates a new State instance, initializing it with data
// loaded from the specified file. This allows for resuming a previous backup operation.
func newStateFromFile(
	ctx context.Context,
	config *ConfigBackup,
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
	// Skip meta data.
	if n == -1 {
		return nil
	}

	file, err := s.writer.NewWriter(s.ctx, s.FileName, false)
	if err != nil {
		return fmt.Errorf("failed to create state file %s: %w", s.FileName, err)
	}

	enc := gob.NewEncoder(file)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.RecordStatesSaved[n] = s.RecordStates[n]

	if err = enc.Encode(s); err != nil {
		return fmt.Errorf("failed to encode state data: %w", err)
	}

	if err = file.Close(); err != nil {
		return fmt.Errorf("failed to close state file: %w", err)
	}

	s.logger.Debug("state file dumped", slog.String("path", s.FileName), slog.Time("saved at", time.Now()))

	return nil
}

func (s *State) initState(pf []*a.PartitionFilter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range pf {
		pfs, err := models.NewPartitionFilterSerialized(pf[i])
		if err != nil {
			return err
		}

		s.RecordStates[i] = pfs
		s.RecordStatesSaved[i] = pfs
	}

	return nil
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

// cleanup removes the state file.
// Must be called when backup is completed.
func (s *State) cleanup(ctx context.Context) error {
	if err := s.writer.Remove(ctx, s.FilePath); err != nil {
		return fmt.Errorf("failed to remove state file: %w", err)
	}

	return nil
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
