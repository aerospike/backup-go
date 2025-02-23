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

import "testing"

func TestConfigBackupXDR_validate(t *testing.T) {
	tests := []struct {
		name    string
		config  ConfigBackupXDR
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: ConfigBackupXDR{
				DC:               "dc1",
				LocalAddress:     "127.0.0.1",
				LocalPort:        3000,
				Namespace:        "test",
				Rewind:           "all",
				ParallelWrite:    1,
				ReadTimeout:      1000,
				WriteTimeout:     1000,
				ResultQueueSize:  100,
				AckQueueSize:     100,
				MaxConnections:   10,
				InfoPolingPeriod: 1000,
				EncoderType:      EncoderTypeASBX,
			},
			wantErr: false,
		},
		{
			name: "invalid rewind",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "invalid",
				MaxConnections: 1,
			},
			wantErr: true,
			errMsg:  "rewind must be a positive number or 'all', got: invalid",
		},
		{
			name: "negative file limit",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				FileLimit:      -1,
				MaxConnections: 1,
			},
			wantErr: true,
			errMsg:  "filelimit value must not be negative, got -1",
		},
		{
			name: "invalid parallel write - too low",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				ParallelWrite:  0,
				MaxConnections: 1,
			},
			wantErr: true,
			errMsg:  "parallel write must be between 1 and 1024, got 0",
		},
		{
			name: "empty dc",
			config: ConfigBackupXDR{
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "dc name must not be empty",
		},
		{
			name: "empty local address",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "local address must not be empty",
		},
		{
			name: "invalid port - negative",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      -1,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "local port must be between 0 and 65535, got -1",
		},
		{
			name: "invalid port - too high",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      65536,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "local port must be between 0 and 65535, got 65536",
		},
		{
			name: "empty namespace",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "namespace must not be empty",
		},
		{
			name: "negative read timeout",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				ReadTimeout:    -1,
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "read timeout must not be negative, got -1",
		},
		{
			name: "negative write timeout",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				WriteTimeout:   -1,
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "write timeout must not be negative, got -1",
		},
		{
			name: "negative result queue size",
			config: ConfigBackupXDR{
				DC:              "dc1",
				LocalAddress:    "127.0.0.1",
				LocalPort:       3000,
				Namespace:       "test",
				Rewind:          "all",
				ResultQueueSize: -1,
				MaxConnections:  1,
				ParallelWrite:   1,
			},
			wantErr: true,
			errMsg:  "result queue size must not be negative, got -1",
		},
		{
			name: "negative ack queue size",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				AckQueueSize:   -1,
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "ack queue size must not be negative, got -1",
		},
		{
			name: "negative info polling period",
			config: ConfigBackupXDR{
				DC:               "dc1",
				LocalAddress:     "127.0.0.1",
				LocalPort:        3000,
				Namespace:        "test",
				Rewind:           "all",
				InfoPolingPeriod: -1,
				MaxConnections:   1,
				ParallelWrite:    1,
			},
			wantErr: true,
			errMsg:  "info poling period must not be less than 1, got -1",
		},
		{
			name: "max connections less than 1",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 0,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "max connections must not be less than 1, got 0",
		},
		{
			name: "parallel write too high",
			config: ConfigBackupXDR{
				DC:             "dc1",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				ParallelWrite:  1025,
				MaxConnections: 1,
			},
			wantErr: true,
			errMsg:  "parallel write must be between 1 and 1024, got 1025",
		},
		{
			name: "dc name too long (32 chars)",
			config: ConfigBackupXDR{
				DC:             "abcdefghijklmnopqrstuvwxyz123456",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "dc name must be less than 32 characters",
		},
		{
			name: "dc name with invalid characters",
			config: ConfigBackupXDR{
				DC:             "dc@name",
				LocalAddress:   "127.0.0.1",
				LocalPort:      3000,
				Namespace:      "test",
				Rewind:         "all",
				MaxConnections: 1,
				ParallelWrite:  1,
			},
			wantErr: true,
			errMsg:  "dc name must match ^[a-zA-Z0-9_\\-$]+$",
		},
		{
			name: "dc name with valid special chars",
			config: ConfigBackupXDR{
				DC:               "dc-name_$123",
				LocalAddress:     "127.0.0.1",
				LocalPort:        3000,
				Namespace:        "test",
				Rewind:           "all",
				MaxConnections:   1,
				ParallelWrite:    1,
				InfoPolingPeriod: 1,
				EncoderType:      EncoderTypeASBX,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ConfigBackupXDR.validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("ConfigBackupXDR.validate() error message = %v, want %v", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestValidateRewind(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid all",
			value:   "all",
			wantErr: false,
		},
		{
			name:    "valid number",
			value:   "100",
			wantErr: false,
		},
		{
			name:    "invalid zero",
			value:   "0",
			wantErr: true,
			errMsg:  "rewind must be a positive number or 'all', got: 0",
		},
		{
			name:    "invalid negative",
			value:   "-1",
			wantErr: true,
			errMsg:  "rewind must be a positive number or 'all', got: -1",
		},
		{
			name:    "invalid string",
			value:   "invalid",
			wantErr: true,
			errMsg:  "rewind must be a positive number or 'all', got: invalid",
		},
		{
			name:    "empty string",
			value:   "",
			wantErr: true,
			errMsg:  "rewind must be a positive number or 'all', got: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRewind(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRewind() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("validateRewind() error message = %v, want %v", err.Error(), tt.errMsg)
			}
		})
	}
}
