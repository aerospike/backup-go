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

const (
	testNamespace = "test"
	testDir       = "test-dir"
	testFile      = "test-file"
)

func TestValidateBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		backup      *Backup
		wantErr     bool
		expectedErr string
	}{
		{
			name: "Both AfterDigest and PartitionList configured",
			backup: &Backup{
				AfterDigest:   "some-digest",
				PartitionList: "some-partition",
				Common: Common{
					Directory: testDir,
				},
			},
			wantErr:     true,
			expectedErr: "only one of after-digest or partition-list can be configured",
		},
		{
			name: "Only AfterDigest configured",
			backup: &Backup{
				AfterDigest:   "some-digest",
				PartitionList: "",
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Only PartitionList configured",
			backup: &Backup{
				AfterDigest:   "",
				PartitionList: "some-partition",
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Neither AfterDigest nor PartitionList configured",
			backup: &Backup{
				AfterDigest:   "",
				PartitionList: "",
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Estimate with PartitionList",
			backup: &Backup{
				Estimate:      true,
				PartitionList: "some-partition",
			},

			wantErr:     true,
			expectedErr: "estimate with any filter is not allowed",
		},
		{
			name: "Estimate with output file",
			backup: &Backup{
				Estimate:   true,
				OutputFile: testFile,
			},

			wantErr:     true,
			expectedErr: "estimate with output-file or directory is not allowed",
		},
		{
			name: "Estimate with valid configuration",
			backup: &Backup{
				Estimate:        true,
				EstimateSamples: 100,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Estimate with invalid samples size",
			backup: &Backup{
				Estimate:        true,
				EstimateSamples: -1,
			},

			wantErr:     true,
			expectedErr: "estimate with estimate-samples < 0 is not allowed",
		},
		{
			name: "Non-estimate with no output or directory",
			backup: &Backup{
				Estimate:   false,
				OutputFile: "",
				Common:     Common{Directory: ""},
			},
			wantErr:     true,
			expectedErr: "must specify either output-file or directory",
		},
		{
			name: "Non-estimate with output file",
			backup: &Backup{
				Estimate:   false,
				OutputFile: testFile,
				Common: Common{
					Directory: "",
					Namespace: testNamespace,
				},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Non-estimate with directory",
			backup: &Backup{
				Estimate:   false,
				OutputFile: "",
				Common: Common{
					Directory: testDir,
					Namespace: testNamespace,
				},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Continue and nodes",
			backup: &Backup{
				StateFileDst:  "some-file",
				ParallelNodes: true,
				OutputFile:    testFile,
			},

			wantErr:     true,
			expectedErr: "saving states and calculating estimates is not possible in parallel node mode",
		},
		{
			name: "Continue with valid state file",
			backup: &Backup{
				Continue:   "state.json",
				OutputFile: testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "NodeList with parallel nodes",
			backup: &Backup{
				NodeList:      "node1,node2",
				ParallelNodes: true,
				OutputFile:    testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "FilterExpression with valid expression",
			backup: &Backup{
				FilterExpression: "age > 25",
				OutputFile:       testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "Modified time filters",
			backup: &Backup{
				ModifiedAfter:  "2024-01-01",
				ModifiedBefore: "2024-12-31",
				OutputFile:     testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "NoTTLOnly flag",
			backup: &Backup{
				NoTTLOnly:  true,
				OutputFile: testFile,
				Common: Common{
					Namespace: testNamespace,
				},
			},

			wantErr: false,
		},
		{
			name: "Estimate with FilterExpression",
			backup: &Backup{
				Estimate:         true,
				FilterExpression: "age > 25",
			},

			wantErr:     true,
			expectedErr: "estimate with any filter is not allowed",
		},
		{
			name: "Estimate with ModifiedAfter",
			backup: &Backup{
				Estimate:      true,
				ModifiedAfter: "2024-01-01",
			},

			wantErr:     true,
			expectedErr: "estimate with any filter is not allowed",
		},
		{
			name: "Both directory and output file configured",
			backup: &Backup{
				OutputFile: testFile,
				Common:     Common{Directory: testDir},
			},
			wantErr:     true,
			expectedErr: "only one of output-file and directory may be configured at the same time",
		},
		{
			name: "Both node-list and rack-list configured",
			backup: &Backup{
				NodeList: "1,2",
				RackList: "3,4",
				Common:   Common{Directory: testDir},
			},
			wantErr:     true,
			expectedErr: "specify either rack-list or node-list, but not both",
		},
		{
			name: "Both continue and state-file-dst configured",
			backup: &Backup{
				Continue:     "state",
				StateFileDst: "state",
				Common:       Common{Directory: testDir},
			},
			wantErr:     true,
			expectedErr: "continue and state-file-dst are mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.backup.Validate()
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
