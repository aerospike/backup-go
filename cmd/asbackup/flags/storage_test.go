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

func TestStorage_NewFlagSet(t *testing.T) {
	t.Parallel()
	storage := NewStorage()

	flagSet := storage.NewFlagSet()

	args := []string{
		"--Directory", "/backup/directory",
		"--output-file", "backup-file.bak",
		"--remove-files",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := storage.GetStorage()

	assert.Equal(t, "/backup/directory", result.Directory)
	assert.Equal(t, "backup-file.bak", result.OutputFile)
	assert.True(t, result.RemoveFiles)
}

func TestStorage_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	storage := NewStorage()

	flagSet := storage.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := storage.GetStorage()

	assert.Equal(t, "", result.Directory)
	assert.Equal(t, "", result.OutputFile)
	assert.False(t, result.RemoveFiles)
}
