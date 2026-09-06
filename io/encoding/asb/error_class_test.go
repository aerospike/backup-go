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

package asb

import (
	"strings"
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

func TestErrorClasses_Corrupt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "invalid token sentinel",
			call: func() error { return errInvalidToken },
		},
		{
			name: "malformed version",
			call: func() error {
				_, err := parseVersion("not-a-version")
				return err
			},
		},
		{
			name: "malformed number",
			call: func() error {
				_, err := readUnsignedInt(newCountingReader(strings.NewReader("12x\n"), "test"), '\n')
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, tt.call(), models.ErrCorruptData)
		})
	}
}

func TestValidator_Unsupported(t *testing.T) {
	t.Parallel()

	err := NewValidator().Run("backup.txt")

	require.ErrorIs(t, err, models.ErrUnsupported)
	require.ErrorContains(t, err, "expected extension")
}
