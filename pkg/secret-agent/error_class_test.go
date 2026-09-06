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

//nolint:revive,nolintlint // We want to use package name with underscore.
package secret_agent

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testResource       = "resource"
	testUnreachableTCP = "127.0.0.1:1"
	testMissingSocket  = "/tmp/backup-go-no-such.sock"
	testShortTimeout   = 100 * time.Millisecond
)

func TestNewClient_UnsupportedConnectionType(t *testing.T) {
	t.Parallel()

	_, err := NewClient(ConnectionTypeUDS, testMissingSocket, testShortTimeout, false,
		&tls.Config{MinVersion: tls.VersionTLS12})

	require.ErrorIs(t, err, models.ErrUnsupported)
}

func TestGetSecret_Unreachable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		connectionType string
		address        string
	}{
		// Port 1 on the loopback interface is not listening in any sane
		// environment, so the dial fails fast instead of hanging.
		{name: "tcp", connectionType: ConnectionTypeTCP, address: testUnreachableTCP},
		{name: "uds", connectionType: ConnectionTypeUDS, address: testMissingSocket},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewClient(tt.connectionType, tt.address, testShortTimeout, false, nil)
			require.NoError(t, err)

			_, err = client.GetSecret(t.Context(), testResource, testSecretKey)

			require.ErrorIs(t, err, models.ErrSecretAgent)
		})
	}
}
