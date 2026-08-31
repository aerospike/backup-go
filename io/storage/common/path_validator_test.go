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

package common

import (
	"testing"
)

func TestValidateObjectKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"empty", "", false},
		{"single_component", "backup_0.asb", false},
		{"nested_key", "folder/backup_0.asb", false},
		{"leading_slash", "/absolute/key", true},
		{"dotdot_segment", "folder/../escape", true},
		{"null", "key\x00.asb", true},
		{"backslash", "folder\\file", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateObjectKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateObjectKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
