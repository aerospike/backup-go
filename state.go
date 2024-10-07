package backup

import (
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// State contains current backups status data.
type State struct {
	// Global backup context.
	ctx context.Context

	// File to save to.
	fileName string

	// How often file will be saved to disk.
	dumpTimeout time.Duration

	// List of applied partition filters
	PartitionFilters []*a.PartitionFilter

	// Save files cursor.
	// TODO: think how to map it to filters.

	// timestamp of last dump to file.
	SavedAt time.Time

	// logger for logging errors.
	logger *slog.Logger
}

// NewState creates status service from parameters, for backup operations.
func NewState(
	ctx context.Context,
	fileName string,
	dumpTimeout time.Duration,
	partitionFilters []*a.PartitionFilter,
	logger *slog.Logger,
) *State {
	s := &State{
		ctx:              ctx,
		fileName:         fileName,
		dumpTimeout:      dumpTimeout,
		PartitionFilters: partitionFilters,
		logger:           logger,
	}
	// Run watcher on initialization.
	go s.serve()

	return s
}

// NewStateFromFile creates a status service from the file, for restore operations.
func NewStateFromFile(ctx context.Context, fileName string, logger *slog.Logger) (*State, error) {
	// TODO: replace with io reader/writer.
	reader, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}

	dec := gob.NewDecoder(reader)

	var state State
	if err = dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}

	state.ctx = ctx
	state.logger = logger

	return &state, nil
}

// serve dumps files to disk.
func (s *State) serve() {
	ticker := time.NewTicker(s.dumpTimeout)
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

			return
		case <-ticker.C:
			// save state and sleep.
			time.Sleep(time.Second)
			// save intermediate state.
			if err := s.dump(); err != nil {
				s.logger.Error("failed to dump state", slog.Any("error", err))
				return
			}

			s.SavedAt = time.Now()
		}
	}
}

func (s *State) dump() error {
	// TODO: replace with io reader/writer.
	file, err := os.OpenFile(s.fileName, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create state file %s: %w", s.fileName, err)
	}

	enc := gob.NewEncoder(file)

	// TODO: check if we must create copies from PartitionFilters.

	if err = enc.Encode(s); err != nil {
		return fmt.Errorf("failed to encode state data: %w", err)
	}

	return nil
}
