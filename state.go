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
	// Is used to create prefix for backup files.
	Counter int
	// RecordsChan communication channel to save current filter state.
	RecordsChan chan models.PartitionFilterSerialized
	// RecordStates store states of all filters.
	RecordStates map[string]models.PartitionFilterSerialized
	// Mutex for RecordStates operations.
	// Ordinary mutex is used, because we must not allow any writings when we read state.
	mu sync.Mutex
	// File to save state to.
	FileName string
	// How often file will be saved to disk.
	DumpDuration time.Duration

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
		return newState(ctx, config, writer, logger), nil
	case config.isStateContinue():
		s, err := newStateFromFile(ctx, config, reader, writer, logger)
		if err != nil {
			return nil, err
		}
		// change filters in config.
		config.PartitionFilters, err = s.loadPartitionFilters()
		if err != nil {
			return nil, err
		}

		return s, nil
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
		// RecordsChan must not be buffered, so we can stop all operations.
		RecordsChan:  make(chan models.PartitionFilterSerialized),
		RecordStates: make(map[string]models.PartitionFilterSerialized),
		FileName:     config.StateFile,
		DumpDuration: config.StateFileDumpDuration,
		writer:       writer,
		logger:       logger,
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
	s.RecordsChan = make(chan models.PartitionFilterSerialized)
	s.Counter++

	logger.Debug("loaded state file successfully")

	// Run watcher on initialization.
	go s.serve()
	go s.serveRecords()

	return &s, nil
}

// serve dumps files to disk.
func (s *State) serve() {
	ticker := time.NewTicker(s.DumpDuration)
	defer ticker.Stop()

	// Dump a file at the very beginning.
	if err := s.dump(); err != nil {
		s.logger.Error("failed to dump state", slog.Any("error", err))
		return
	}

	// Server ticker.
	for {
		select {
		case <-s.ctx.Done():
			// saves state and exit
			if err := s.dump(); err != nil {
				s.logger.Error("failed to dump state", slog.Any("error", err))
				return
			}

			s.logger.Debug("state context done")

			return
		case <-ticker.C:
			// save intermediate state.
			if err := s.dump(); err != nil {
				s.logger.Error("failed to dump state", slog.Any("error", err))
				return
			}
		}
	}
}

func (s *State) dump() error {
	file, err := s.writer.NewWriter(s.ctx, s.FileName)
	if err != nil {
		return fmt.Errorf("failed to create state file %s: %w", s.FileName, err)
	}

	enc := gob.NewEncoder(file)

	s.mu.Lock()
	if err = enc.Encode(s); err != nil {
		return fmt.Errorf("failed to encode state data: %w", err)
	}
	// file.Close()
	s.mu.Unlock()

	s.logger.Debug("state file dumped", slog.Time("saved at", time.Now()))

	return nil
}

func (s *State) loadPartitionFilters() ([]*a.PartitionFilter, error) {
	s.mu.Lock()

	result := make([]*a.PartitionFilter, 0, len(s.RecordStates))

	for _, state := range s.RecordStates {
		f, err := state.Decode()
		if err != nil {
			return nil, err
		}

		result = append(result, f)
	}

	s.mu.Unlock()

	return result, nil
}

func (s *State) serveRecords() {
	var counter int

	for {
		select {
		case <-s.ctx.Done():
			return
		case state := <-s.RecordsChan:
			if state.Begin == 0 && state.Count == 0 && state.Digest == nil {
				continue
			}

			counter++

			s.mu.Lock()
			key := fmt.Sprintf("%d%d%s", state.Begin, state.Count, state.Digest)
			s.RecordStates[key] = state
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
	readCh := make(chan io.ReadCloser)
	errCh := make(chan error)

	go reader.StreamFile(ctx, fileName, readCh, errCh)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errCh:
			return nil, err
		case file := <-readCh:
			return file, nil
		}
	}
}
