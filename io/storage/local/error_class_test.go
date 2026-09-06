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

package local

import (
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/stretchr/testify/require"
)

func TestValidateFilename_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
	}{
		{name: "current directory", filename: "."},
		{name: "parent directory", filename: ".."},
		{name: "path separator", filename: "dir/backup.asb"},
		{name: "nul byte", filename: "back\x00up.asb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, ValidateFilename(tt.filename), errclass.ErrInvalidConfig)
		})
	}
}

func TestReader_MissingDirectory_NotFound(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "does-not-exist")

	_, err := NewReader(t.Context(), options.WithDir(missing))

	require.ErrorIs(t, err, errclass.ErrNotFound)
}
