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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateCommon(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		common      *Common
		wantErr     bool
		expectedErr string
	}{
		{
			name: "Valid total and socket timeout",
			common: &Common{
				TotalTimeout:  1000,
				SocketTimeout: 500,
				Namespace:     testNamespace,
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Invalid negative total timeout",
			common: &Common{
				TotalTimeout:  -1,
				SocketTimeout: 500,
				Namespace:     testNamespace,
			},
			wantErr:     true,
			expectedErr: "total-timeout must be non-negative",
		},
		{
			name: "Invalid negative socket timeout",
			common: &Common{
				TotalTimeout:  1000,
				SocketTimeout: -1,
				Namespace:     testNamespace,
			},
			wantErr:     true,
			expectedErr: "socket-timeout must be non-negative",
		},
		{
			name: "Both total and socket timeout negative",
			common: &Common{
				TotalTimeout:  -1000,
				SocketTimeout: -500,
				Namespace:     testNamespace,
			},
			wantErr:     true,
			expectedErr: "total-timeout must be non-negative",
		},
		{
			name: "Edge case: zero total and socket timeout",
			common: &Common{
				TotalTimeout:  0,
				SocketTimeout: 0,
				Namespace:     testNamespace,
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Missing namespace",
			common: &Common{
				TotalTimeout:  1000,
				SocketTimeout: 500,
				Namespace:     "",
			},
			wantErr:     true,
			expectedErr: "namespace is required",
		},
		{
			name: "Valid namespace with all timeouts",
			common: &Common{
				TotalTimeout:  1000,
				SocketTimeout: 500,
				Namespace:     "test-ns",
			},
			wantErr:     false,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.common.Validate()
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
