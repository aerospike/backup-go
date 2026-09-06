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
	"fmt"
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorClasses_Aliases pins the root package aliases to the canonical
// values in models, so errors.Is matches either spelling.
func TestErrorClasses_Aliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		alias     error
		canonical error
	}{
		{name: "invalid config", alias: ErrInvalidConfig, canonical: models.ErrInvalidConfig},
		{name: "not found", alias: ErrNotFound, canonical: models.ErrNotFound},
		{name: "storage", alias: ErrStorage, canonical: models.ErrStorage},
		{name: "corrupt data", alias: ErrCorruptData, canonical: models.ErrCorruptData},
		{name: "unsupported", alias: ErrUnsupported, canonical: models.ErrUnsupported},
		{name: "aerospike", alias: ErrAerospike, canonical: models.ErrAerospike},
		{name: "secret agent", alias: ErrSecretAgent, canonical: models.ErrSecretAgent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Sentinels have no Unwrap, so matching both ways proves identity.
			require.ErrorIs(t, tt.alias, tt.canonical)
			require.ErrorIs(t, tt.canonical, tt.alias)

			wrapped := fmt.Errorf("some context: %w", fmt.Errorf("%w: details", tt.canonical))
			assert.ErrorIs(t, wrapped, tt.alias)
		})
	}
}

// TestErrorClasses_Distinct guards against a class accidentally being defined
// in terms of another one: matching must stay exact.
func TestErrorClasses_Distinct(t *testing.T) {
	t.Parallel()

	classes := []error{
		ErrInvalidConfig,
		ErrNotFound,
		ErrStorage,
		ErrCorruptData,
		ErrUnsupported,
		ErrAerospike,
		ErrSecretAgent,
	}

	for i, outer := range classes {
		for j, inner := range classes {
			if i == j {
				continue
			}

			assert.NotErrorIs(t, outer, inner, "class %d must not match class %d", i, j)
		}
	}
}

// TestClientErrors_InvalidConfig covers the client entry points that reject a
// caller mistake before any work is started.
func TestClientErrors_InvalidConfig(t *testing.T) {
	t.Parallel()

	client := &Client{}

	tests := []struct {
		name    string
		call    func() error
		wantErr string
	}{
		{
			name: "backup without config",
			call: func() error {
				_, err := client.Backup(t.Context(), nil, nil, nil)
				return err
			},
			wantErr: "backup config required",
		},
		{
			name: "restore without config",
			call: func() error {
				_, err := client.Restore(t.Context(), nil, nil)
				return err
			},
			wantErr: "restore config required",
		},
		{
			name: "estimate without config",
			call: func() error {
				_, err := client.Estimate(t.Context(), nil, 1)
				return err
			},
			wantErr: "backup config required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestPartitionFilterParsing_InvalidConfig covers the exported partition
// filter helpers, which validate arguments without going through a config.
func TestPartitionFilterParsing_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		call    func() error
		wantErr string
	}{
		{
			name: "empty filter list",
			call: func() error {
				_, err := ParsePartitionFilterListString("test", "")
				return err
			},
			wantErr: "empty filters",
		},
		{
			name: "invalid digest",
			call: func() error {
				_, err := NewPartitionFilterByDigest("test", "not-base64!")
				return err
			},
			wantErr: "digest",
		},
		{
			name: "unparsable filter string",
			call: func() error {
				_, err := ParsePartitionFilterListString("test", "not-a-filter")
				return err
			},
			wantErr: "partition filter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestErrorClass_SurvivesWrapping documents the contract callers rely on: the
// class stays reachable no matter how much context is added on the way out.
func TestErrorClass_SurvivesWrapping(t *testing.T) {
	t.Parallel()

	leaf := fmt.Errorf("%w: bad thing", models.ErrStorage)
	wrapped := fmt.Errorf("failed to write chunk: %w", fmt.Errorf("failed to open file: %w", leaf))

	require.ErrorIs(t, wrapped, ErrStorage)
	require.NotErrorIs(t, wrapped, ErrCorruptData)
	require.Contains(t, wrapped.Error(), "failed to write chunk")
	require.Contains(t, wrapped.Error(), "bad thing")
}
