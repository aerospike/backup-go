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

package common

import (
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const testObjectKey = "backup.asb"

func TestErrorClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		err   error
		class error
	}{
		{name: "empty storage", err: ErrEmptyStorage, class: models.ErrNotFound},
		{name: "archived object", err: ErrArchivedObject, class: models.ErrStorage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, tt.err, tt.class)
		})
	}
}

func TestValidateObjectKey_InvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
	}{
		{name: "nul byte", key: "back\x00up.asb"},
		{name: "leading slash", key: "/" + testObjectKey},
		{name: "parent segment", key: "../" + testObjectKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, ValidateObjectKey(tt.key), models.ErrInvalidConfig)
		})
	}
}

func TestGetFullPath_InvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := GetFullPath("", testObjectKey, nil, false)
	require.ErrorIs(t, err, models.ErrInvalidConfig)
}
