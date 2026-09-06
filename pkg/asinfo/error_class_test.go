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

package asinfo

import (
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

func TestErrorClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class error
	}{
		{name: "replication factor zero", err: ErrReplicationFactorZero, class: models.ErrAerospike},
		{name: "no node", err: ErrNoNode, class: models.ErrNotFound},
		{name: "not found", err: ErrNotFound, class: models.ErrNotFound},
		{name: "invalid sindex type", err: ErrInvalidSIndexType, class: models.ErrCorruptData},
		{name: "no info commands", err: errNoInfoCommands, class: models.ErrInvalidConfig},
		{name: "no nodes available", err: errNoNodesAvailable, class: models.ErrAerospike},
		{name: "no nodes connected", err: errNoNodesConnected, class: models.ErrAerospike},
		{name: "replication factor not found", err: errReplicationFactorNotFound, class: models.ErrAerospike},
		{name: "parse record info", err: errParseRecordInfo, class: models.ErrAerospike},
		{name: "udf missing filename", err: errUDFMissingFilename, class: models.ErrAerospike},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, tt.err, tt.class)
		})
	}
}

func TestGetInfo_NoCommands(t *testing.T) {
	t.Parallel()

	ic := &Client{}

	_, err := ic.GetInfo(t.Context(), "")

	require.ErrorIs(t, err, models.ErrInvalidConfig)
}
