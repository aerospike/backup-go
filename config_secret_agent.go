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
)

// SecretAgentConfig contains Secret Agent connection information.
type SecretAgentConfig struct {
	// Connection type: tcp, unix.
	// Use constants form `secret-agent`: `ConnectionTypeTCP` or `ConnectionTypeUDS`
	ConnectionType *string `yaml:"sa-connection-type,omitempty" json:"sa-connection-type,omitempty"`
	// Secret agent host for TCP connection or socket file path for UDS connection.
	Address *string `yaml:"sa-address,omitempty" json:"sa-address,omitempty"`
	// Secret agent port (only for TCP connection).
	Port *int `yaml:"sa-port,omitempty" json:"sa-port,omitempty"`
	// Secret agent connection and reading timeout.
	// Default: 1000 millisecond.
	TimeoutMillisecond *int `yaml:"sa-timeout-millisecond,omitempty" json:"sa-timeout-millisecond,omitempty"`
	// Path to ca file for encrypted connection.
	CaFile *string `yaml:"sa-ca-file,omitempty" json:"sa-ca-file,omitempty"`
	// Flag that shows if secret agent responses are encrypted with base64.
	IsBase64 *bool `yaml:"sa-is-base64,omitempty" json:"sa-is-base64,omitempty"`
}

// Validate validates the SecretAgentConfig.
func (s *SecretAgentConfig) Validate() error {
	if s == nil {
		return nil
	}

	// As Secret Agent configuration is not mandatory, we will validate params
	// only if the secret agent is enabled.
	// If ConnectionType is set, we assume that the secret agent is enabled.
	if s.ConnectionType != nil {
		if s.Address == nil {
			return fmt.Errorf("secret agent address is required")
		}
	}

	return nil
}
