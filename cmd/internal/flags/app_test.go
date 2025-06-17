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

	app := NewApp()

	flagSet := app.NewFlagSet()

	args := []string{
		"--version",
		"--verbose",
		"--log-level", "error",
		"--log-json",
		"--config", "config.yaml",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	assert.False(t, app.Help, "Help flag should default to false")
	assert.True(t, app.Version, "Version flag should be true when set")
	assert.True(t, app.Verbose, "Verbose flag should be true when set")
	assert.Equal(t, app.LogLevel, "error", "Log level flag should be error")
	assert.True(t, app.LogJSON, "Log JSON flag should be true when set")
	assert.Equal(t, app.Config, "config.yaml", "Config flag should be config.yaml")
}

func TestApp_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	app := NewApp()

	flagSet := app.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	assert.False(t, app.Help, "Help flag should default to false")
	assert.False(t, app.Version, "Version flag should default to false")
	assert.False(t, app.Verbose, "Verbose flag should default to false")
	assert.Equal(t, app.LogLevel, "debug", "Log level flag should default be debug")
	assert.False(t, app.LogJSON, "Log JSON flag should default to false")
	assert.Equal(t, app.Config, "", "Config flag should default should be empty string")
}
