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

func TestApp_NewFlagSet(t *testing.T) {
	t.Parallel()
	// Create a new App object
	app := NewApp()

	// Create a new FlagSet
	flagSet := app.NewFlagSet()

	// Simulate passing flags via command-line arguments
	args := []string{
		"--version",
		"--verbose",
	}

	// Parse the arguments
	err := flagSet.Parse(args)
	assert.NoError(t, err)

	// Check if the flags were parsed correctly
	assert.False(t, app.Help, "Help flag should default to false")
	assert.True(t, app.Version, "Version flag should be true when set")
	assert.True(t, app.Verbose, "Verbose flag should be true when set")
}

func TestApp_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	// Create a new App object
	app := NewApp()

	// Create a new FlagSet without setting any arguments (to test defaults)
	flagSet := app.NewFlagSet()

	// Parse with no arguments to use default values
	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	// Verify default values
	assert.False(t, app.Help, "Help flag should default to false")
	assert.False(t, app.Version, "Version flag should default to false")
	assert.False(t, app.Verbose, "Verbose flag should default to false")
}
