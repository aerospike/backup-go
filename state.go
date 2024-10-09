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
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// Must be the same as pipeline channelSize
const channelSize = 256

// State contains current backups status data.
type State struct {
	// Global backup context.
	ctx context.Context

	// File to save to.
	FileName string

	// How often file will be saved to disk.
	DumpDuration time.Duration

	// Config *BackupConfig

	// logger for logging errors.
	logger *slog.Logger

	// ------ experiments -----
	RecordsChan  chan recordState
	RecordStates map[string]recordState
	mu           sync.RWMutex
}

type recordState struct {
	Filter filter
}

// filter contains custom filter struct to save filter to GOB.
type filter struct {
	Begin  int
	Count  int
	Digest []byte
	Cursor []byte
}

func mapToFilter(pf a.PartitionFilter) (filter, error) {
	c, err := pf.EncodeCursor()
	if err != nil {
		return filter{}, fmt.Errorf("failed to encode cursor: %w", err)
	}
	return filter{
		Begin:  pf.Begin,
		Count:  pf.Count,
		Digest: pf.Digest,
		Cursor: c,
	}, nil
}

func mapFromFilter(f filter) (*a.PartitionFilter, error) {
	pf := &a.PartitionFilter{Begin: f.Begin, Count: f.Count, Digest: f.Digest}
	if err := pf.DecodeCursor(f.Cursor); err != nil {
		return nil, fmt.Errorf("failed to decode cursor: %w", err)
	}

	return pf, nil
}

func newRecordState(filter a.PartitionFilter) recordState {
	f, err := mapToFilter(filter)
	if err != nil {
		log.Fatalf("failed to map partition filter: %w", err)
	}
	return recordState{
		Filter: f,
	}
}

func NewState(ctx context.Context,
	config *BackupConfig,
	logger *slog.Logger,
) (*State, error) {
	switch {
	case config.isStateFirstRun():
		return newState(ctx, config, logger), nil
	case config.isStateContinue():
		s, err := newStateFromFile(ctx, config, logger)
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

// NewState creates status service from parameters, for backup operations.
func newState(
	ctx context.Context,
	config *BackupConfig,
	logger *slog.Logger,
) *State {

	s := &State{
		ctx:          ctx,
		FileName:     config.StateFile,
		DumpDuration: config.StateFileDumpDuration,
		//		Config:       config,
		logger:       logger,
		RecordsChan:  make(chan recordState, channelSize),
		RecordStates: make(map[string]recordState),
	}
	// Run watcher on initialization.
	go s.serve()
	go s.serveRecords()

	return s
}

// NewStateFromFile creates a status service from the file, to continue operations.
func newStateFromFile(
	ctx context.Context,
	config *BackupConfig,
	logger *slog.Logger,
) (*State, error) {
	// TODO: replace with io reader/writer.
	reader, err := os.Open(config.StateFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}

	dec := gob.NewDecoder(reader)

	var s State
	if err = dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}

	s.ctx = ctx
	s.logger = logger
	s.RecordsChan = make(chan recordState, channelSize)

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
			// save state and sleep.
			time.Sleep(time.Second)
			// save intermediate state.
			if err := s.dump(); err != nil {
				s.logger.Error("failed to dump state", slog.Any("error", err))
				return
			}
		}
	}
}

func (s *State) dump() error {
	// TODO: replace with io reader/writer.
	file, err := os.OpenFile(s.FileName, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create state file %s: %w", s.FileName, err)
	}

	enc := gob.NewEncoder(file)
	s.mu.RLock()
	if err = enc.Encode(s); err != nil {
		return fmt.Errorf("failed to encode state data: %w", err)
	}
	s.mu.RUnlock()

	s.logger.Debug("state file dumped", slog.Time("saved at", time.Now()))

	return nil
}

func (s *State) loadPartitionFilters() ([]*a.PartitionFilter, error) {
	s.mu.RLock()

	result := make([]*a.PartitionFilter, 0, len(s.RecordStates))
	for _, state := range s.RecordStates {
		f, err := mapFromFilter(state.Filter)
		if err != nil {
			return nil, err
		}

		result = append(result, f)
	}

	s.mu.RUnlock()

	return result, nil
}

func (s *State) serveRecords() {

	var counter int
	for {
		select {
		case <-s.ctx.Done():
			return
		case state := <-s.RecordsChan:
			counter++
			s.mu.Lock()
			key := fmt.Sprintf("%d%d%s", state.Filter.Begin, state.Filter.Count, state.Filter.Digest)
			s.RecordStates[key] = state
			s.mu.Unlock()

			// if counter == 400000 {
			// 	s.dump()
			// 	fmt.Println("done 4000000")
			// 	os.Exit(1)
			// }
		}
	}
}
