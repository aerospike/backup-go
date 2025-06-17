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

func TestValidateRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		restore *Restore
		wantErr bool
		errMsg  string
	}{
		{
			name: "Valid restore configuration with input file",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      RestoreModeASB,
				Common: Common{
					Namespace: testNamespace,
				},
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory",
			restore: &Restore{
				Mode: RestoreModeASB,
				Common: Common{
					Directory: "restore-dir",
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory list",
			restore: &Restore{
				DirectoryList:   "dir1,dir2",
				ParentDirectory: "parent",
				Mode:            RestoreModeASB,
				Common: Common{
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid restore mode",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      "invalid-mode",
				Common: Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "invalid restore mode: invalid-mode",
		},
		{
			name: "Missing input source",
			restore: &Restore{
				Mode: RestoreModeASB,
				Common: Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "input file or directory required",
		},
		{
			name: "Invalid restore restore - both input file and directory",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      RestoreModeASB,
				Common: Common{
					Directory: "restore-dir",
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "only one of directory and input-file may be configured at the same time",
		},
		{
			name: "Invalid common restore - missing namespace",
			restore: &Restore{
				InputFile: "backup.asb",
				Mode:      RestoreModeASB,
			},
			wantErr: true,
			errMsg:  "namespace is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.restore.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Equal(t, tt.errMsg, err.Error())
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
