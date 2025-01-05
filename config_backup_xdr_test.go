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
				DC:                           "dc1",
				LocalAddress:                 "127.0.0.1",
				LocalPort:                    3000,
				Namespace:                    "test",
				Rewind:                       "all",
				ParallelWrite:                1,
				ReadTimoutMilliseconds:       1000,
				WriteTimeoutMilliseconds:     1000,
				ResultQueueSize:              100,
				AckQueueSize:                 100,
				MaxConnections:               10,
				InfoPolingPeriodMilliseconds: 1000,
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
				DC:                     "dc1",
				LocalAddress:           "127.0.0.1",
				LocalPort:              3000,
				Namespace:              "test",
				Rewind:                 "all",
				ReadTimoutMilliseconds: -1,
				MaxConnections:         1,
				ParallelWrite:          1,
			},
			wantErr: true,
			errMsg:  "read timout must not be negative, got -1",
		},
		{
			name: "negative write timeout",
			config: ConfigBackupXDR{
				DC:                       "dc1",
				LocalAddress:             "127.0.0.1",
				LocalPort:                3000,
				Namespace:                "test",
				Rewind:                   "all",
				WriteTimeoutMilliseconds: -1,
				MaxConnections:           1,
				ParallelWrite:            1,
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
				DC:                           "dc1",
				LocalAddress:                 "127.0.0.1",
				LocalPort:                    3000,
				Namespace:                    "test",
				Rewind:                       "all",
				InfoPolingPeriodMilliseconds: -1,
				MaxConnections:               1,
				ParallelWrite:                1,
			},
			wantErr: true,
			errMsg:  "info poling period must not be negative, got -1",
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