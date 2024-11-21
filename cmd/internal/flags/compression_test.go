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

package flags

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompression_NewFlagSet(t *testing.T) {
	t.Parallel()
	compression := NewCompression(OperationBackup)

	flagSet := compression.NewFlagSet()

	args := []string{
		"--compress", "ZSTD",
		"--compression-level", "5",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := compression.GetCompression()

	assert.Equal(t, "ZSTD", result.Mode, "The compress flag should be parsed correctly")
	assert.Equal(t, 5, result.Level, "The compression-level flag should be parsed correctly")
}

func TestCompression_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	compression := NewCompression(OperationRestore)

	flagSet := compression.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := compression.GetCompression()

	assert.Equal(t, "NONE", result.Mode, "The default value for compress should be 'NONE'")
	assert.Equal(t, 3, result.Level, "The default value for compression-level should be 3")
}
