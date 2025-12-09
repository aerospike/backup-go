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
	"fmt"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
)

// SecretAgentConfig contains Secret Agent connection information.
type SecretAgentConfig struct {
	// Connection type: tcp, unix.
	// Use constants form `secret-agent`: `ConnectionTypeTCP` or `ConnectionTypeUDS`
	ConnectionType *string
	// Secret agent host for TCP connection or socket file path for UDS connection.
	Address *string
	// Secret agent port (only for TCP connection).
	Port *int
	// Secret agent connection and reading timeout.
	// Default: 1000 millisecond.
	TimeoutMillisecond *int
	// Path to a ca file for encrypted connection.
	CaFile *string
	// TLSName used on ca creation (ServerName for SNI).
	TLSName *string
	// Path to a client certificate file for mutual TLS authentication.
	CertFile *string
	// Path to a client private key file for mutual TLS authentication.
	KeyFile *string
	// Flag that shows if secret agent responses are encrypted with base64.
	IsBase64 *bool
}

// validate validates the SecretAgentConfig.
func (s *SecretAgentConfig) validate() error {
	if s == nil {
		return nil
	}

	if s.Address == nil || (s.Address != nil && *s.Address == "") {
		return fmt.Errorf("address is required")
	}

	if s.TimeoutMillisecond != nil && *s.TimeoutMillisecond <= 0 {
		return fmt.Errorf("invalid timeout: %d", *s.TimeoutMillisecond)
	}

	if s.ConnectionType == nil {
		return fmt.Errorf("connection type is required")
	}

	if s.ConnectionType != nil &&
		(*s.ConnectionType != saClient.ConnectionTypeTCP && *s.ConnectionType != saClient.ConnectionTypeUDS) {
		return fmt.Errorf("unsupported connection type: %s", *s.ConnectionType)
	}

	if s.CertFile != nil && s.KeyFile == nil {
		return fmt.Errorf("key file is required when cert file is set")
	}

	if s.KeyFile != nil && s.CertFile == nil {
		return fmt.Errorf("cert file is required when key file is set")
	}

	return nil
}
