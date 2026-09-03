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

package backup

import (
	"testing"

	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/require"
)

func TestNilClientBackup(t *testing.T) {
	t.Parallel()

	c, err := NewClient(nil)
	require.NoError(t, err)

	config := &ConfigBackup{
		Namespace: "test",
	}

	_, err = c.Backup(t.Context(), config, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	require.Error(t, err, "aerospike client is nil")
}

func TestNilClientRestore(t *testing.T) {
	t.Parallel()

	c, err := NewClient(nil)
	require.NoError(t, err)

	config := &ConfigRestore{}

	_, err = c.Restore(t.Context(), config, &mocks.MockStreamingReader{})
	require.Error(t, err, "aerospike client is nil")
}

func TestNilClientEstimates(t *testing.T) {
	t.Parallel()

	c, err := NewClient(nil)
	require.NoError(t, err)

	config := &ConfigBackup{
		Namespace: "test",
	}

	_, err = c.Estimate(t.Context(), config, 100)
	require.Error(t, err, "aerospike client is nil")
}
