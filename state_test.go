// Copyright 2024-2026 Aerospike, Inc.
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
	"bytes"
	"encoding/gob"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testStateFile = "test_state_file"
)

func TestState(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	tempFile := filepath.Join(testDir, testStateFile)

	testFilters := []*a.PartitionFilter{
		NewPartitionFilterByID(1),
		NewPartitionFilterByID(2),
	}

	ctx := t.Context()

	cfg := NewDefaultBackupConfig()
	cfg.StateFile = tempFile
	cfg.PageSize = 100000
	cfg.PartitionFilters = testFilters

	reader, err := local.NewReader(
		ctx,
		options.WithDir(testDir),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	writer, err := local.NewWriter(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithSkipDirCheck(),
		options.WithDir(testDir),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Check init.
	state, err := NewState(ctx, cfg, reader, writer, logger)
	require.NotNil(t, state)
	require.NoError(t, err)

	err = state.initState(testFilters)
	require.NoError(t, err)

	state.Counter = 1
	suf := state.getFileSuffix()
	require.Equal(t, "(1)", suf)

	// Check empty condition.
	state.RecordsStateChan <- models.PartitionFilterSerialized{}
	for i := range testFilters {
		pfs, err := models.NewPartitionFilterSerialized(testFilters[i])
		require.NoError(t, err)
		state.RecordsStateChan <- pfs
	}

	// Create a state file.
	state.SaveCommandChan <- 1
	time.Sleep(1 * time.Second)

	// Check that file exists.
	_, err = os.Stat(tempFile)
	require.NoError(t, err)

	// Check restore.
	newCtx := t.Context()
	cfg.Continue = true
	newState, err := NewState(newCtx, cfg, reader, writer, logger)
	require.NoError(t, err)
	newPf, err := newState.loadPartitionFilters()
	require.NoError(t, err)
	require.Len(t, newPf, len(testFilters))
}

func TestNewStateFromFile_TamperedState(t *testing.T) {
	t.Parallel()

	const (
		testBegin = 10
		testCount = 2
	)

	validCursor := encodeTestCursor(t, testBegin, testCount)
	validFilter := models.PartitionFilterSerialized{Begin: testBegin, Count: testCount, Cursor: validCursor}

	tests := []struct {
		name string
		give []byte
	}{
		{
			name: "no saved filters",
			give: encodeTestState(t, &State{
				Counter:      1,
				RecordStates: map[int]models.PartitionFilterSerialized{0: validFilter},
			}),
		},
		{
			name: "negative begin",
			give: encodeTestSavedFilter(t, models.PartitionFilterSerialized{
				Begin: -1, Count: testCount, Cursor: validCursor,
			}),
		},
		{
			name: "begin out of range",
			give: encodeTestSavedFilter(t, models.PartitionFilterSerialized{
				Begin: MaxPartitions, Count: 1, Cursor: validCursor,
			}),
		},
		{
			name: "negative count",
			give: encodeTestSavedFilter(t, models.PartitionFilterSerialized{
				Begin: testBegin, Count: -1, Cursor: validCursor,
			}),
		},
		{
			name: "zero filter",
			give: encodeTestSavedFilter(t, models.PartitionFilterSerialized{}),
		},
		{
			name: "range exceeds max partitions",
			give: encodeTestSavedFilter(t, models.PartitionFilterSerialized{
				Begin: MaxPartitions - 1, Count: 2, Cursor: validCursor,
			}),
		},
		{
			name: "one invalid filter among valid ones",
			give: encodeTestState(t, &State{Counter: 1, RecordStatesSaved: newTestMixedFilters(t)}),
		},
		{
			name: "negative counter",
			give: encodeTestState(t, &State{
				Counter:           -1,
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{0: validFilter},
			}),
		},
		{
			name: "counter overflow",
			give: encodeTestState(t, &State{
				Counter:           math.MaxInt,
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{0: validFilter},
			}),
		},
		{
			name: "truncated stream",
			give: truncateHalf(encodeTestSavedFilter(t, validFilter)),
		},
		{
			name: "empty file",
			give: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state, err := newTestStateFromBytes(t, tt.give)
			require.ErrorIs(t, err, ErrCorruptData)
			assert.Nil(t, state)
		})
	}
}

func TestNewStateFromFile_ValidState(t *testing.T) {
	t.Parallel()

	const (
		testBegin   = 100
		testCount   = 3
		testCounter = 4
	)

	startedCursor := encodeTestCursor(t, testBegin, testCount)
	emptyCursor := encodeTestCursor(t, testBegin, 0)

	tests := []struct {
		name string
		give *State
	}{
		{
			// gob omits empty maps, so RecordStates is nil after decoding.
			name: "only saved filters",
			give: &State{
				Counter: testCounter,
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					0: {Begin: testBegin, Count: testCount, Cursor: startedCursor},
				},
			},
		},
		{
			name: "scan not started",
			give: &State{
				Counter: testCounter,
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					0: {Begin: testBegin, Count: testCount, Cursor: emptyCursor},
				},
			},
		},
		{
			name: "last partition with digest",
			give: &State{
				Counter: testCounter,
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					0: {
						Begin:  MaxPartitions - 1,
						Count:  1,
						Digest: make([]byte, 20),
						Cursor: encodeTestCursor(t, MaxPartitions-1, 1),
					},
				},
			},
		},
		{
			// A first run writes Counter 0.
			name: "counter of a first run",
			give: &State{
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					0: {Begin: testBegin, Count: testCount, Cursor: startedCursor},
				},
			},
		},
		{
			name: "largest counter",
			give: &State{
				Counter: math.MaxInt - 1,
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					0: {Begin: testBegin, Count: testCount, Cursor: startedCursor},
				},
			},
		},
		{
			// RecordStates from the file is not validated and must be dropped.
			name: "unsaved filters are dropped",
			give: &State{
				Counter: testCounter,
				RecordStates: map[int]models.PartitionFilterSerialized{
					0: {Begin: -1, Count: MaxPartitions + 1},
					3: {Begin: MaxPartitions, Count: -1, N: 3},
				},
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					0: {Begin: testBegin, Count: testCount, Cursor: startedCursor},
				},
			},
		},
		{
			// The writer index is not tied to the parallelism of the continued run.
			name: "writer indexes from a wider run",
			give: &State{
				Counter: testCounter,
				RecordStates: map[int]models.PartitionFilterSerialized{
					7: {Begin: testBegin, Count: testCount, Cursor: startedCursor, N: 7},
				},
				RecordStatesSaved: map[int]models.PartitionFilterSerialized{
					7: {Begin: testBegin, Count: testCount, Cursor: startedCursor, N: 7},
					8: {Begin: 0, Count: MaxPartitions, Cursor: encodeTestCursor(t, 0, MaxPartitions), N: -1},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state, err := newTestStateFromBytes(t, encodeTestState(t, tt.give))
			require.NoError(t, err)
			require.NotNil(t, state)

			assert.Equal(t, tt.give.Counter+1, state.Counter)
			assert.Equal(t, tt.give.RecordStatesSaved, state.RecordStatesSaved)
			assert.Equal(t, tt.give.RecordStatesSaved, state.RecordStates)

			filters, err := state.loadPartitionFilters()
			require.NoError(t, err)
			assert.Len(t, filters, len(tt.give.RecordStatesSaved))
		})
	}
}

func TestStateValidate_ChecksEveryFilter(t *testing.T) {
	t.Parallel()

	s := &State{RecordStatesSaved: newTestMixedFilters(t)}

	// Map iteration order is random: repeat so that the invalid filter is not
	// always the first one visited.
	for range 200 {
		require.Error(t, s.validate())
	}
}

func TestDecodeState_Limit(t *testing.T) {
	t.Parallel()

	data := encodeTestSavedFilter(t, models.PartitionFilterSerialized{
		Begin: 1, Count: 1, Cursor: encodeTestCursor(t, 1, 1),
	})

	tests := []struct {
		name    string
		give    int64
		wantErr bool
	}{
		{name: "limit above size", give: int64(len(data)) + 1},
		{name: "limit equals size", give: int64(len(data))},
		{name: "limit one byte short", give: int64(len(data)) - 1, wantErr: true},
		{name: "zero limit", give: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state, err := decodeState(bytes.NewReader(data), tt.give)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrCorruptData)
				assert.Nil(t, state)

				return
			}

			require.NoError(t, err)
			assert.Len(t, state.RecordStatesSaved, 1)
		})
	}
}

// newTestStateFromBytes writes data as a state file and loads a State from it.
func newTestStateFromBytes(t *testing.T, data []byte) (*State, error) {
	t.Helper()

	testDir := t.TempDir()
	ctx := t.Context()

	cfg := NewDefaultBackupConfig()
	cfg.StateFile = filepath.Join(testDir, testStateFile)
	cfg.Continue = true

	require.NoError(t, os.WriteFile(cfg.StateFile, data, 0o600))

	reader, err := local.NewReader(
		ctx,
		options.WithDir(testDir),
		options.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	writer, err := local.NewWriter(
		ctx,
		options.WithValidator(asb.NewValidator()),
		options.WithSkipDirCheck(),
		options.WithDir(testDir),
	)
	require.NoError(t, err)

	return NewState(ctx, cfg, reader, writer, slog.New(slog.DiscardHandler))
}

// newTestMixedFilters returns saved filters where one invalid filter is hidden
// among valid ones.
func newTestMixedFilters(t *testing.T) map[int]models.PartitionFilterSerialized {
	t.Helper()

	const validFilters = 15

	filters := make(map[int]models.PartitionFilterSerialized, validFilters+1)
	for i := range validFilters {
		filters[i] = models.PartitionFilterSerialized{Begin: i, Count: 1, Cursor: encodeTestCursor(t, i, 1), N: i}
	}

	filters[validFilters] = models.PartitionFilterSerialized{Begin: MaxPartitions, Count: 1, N: validFilters}

	return filters
}

// encodeTestState returns the state file content for s.
func encodeTestState(t *testing.T, s *State) []byte {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(s))

	return buf.Bytes()
}

// encodeTestSavedFilter returns the content of a state file with one saved filter.
func encodeTestSavedFilter(t *testing.T, pfs models.PartitionFilterSerialized) []byte {
	t.Helper()

	return encodeTestState(t, &State{
		Counter:           1,
		RecordStatesSaved: map[int]models.PartitionFilterSerialized{0: pfs},
	})
}

// encodeTestCursor returns a cursor with count partition statuses starting at begin.
func encodeTestCursor(t *testing.T, begin, count int) []byte {
	t.Helper()

	parts := make([]*a.PartitionStatus, count)
	for i := range parts {
		parts[i] = &a.PartitionStatus{Id: begin + i}
	}

	pf := a.PartitionFilter{Partitions: parts}

	cursor, aErr := pf.EncodeCursor()
	require.Nil(t, aErr)

	return cursor
}

func truncateHalf(data []byte) []byte {
	return data[:len(data)/2]
}
