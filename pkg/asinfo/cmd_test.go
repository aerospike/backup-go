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

package asinfo

import (
	"reflect"
	"testing"
)

func TestNewCmdDict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		version        AerospikeVersion
		expectedCmds   map[int]string
		expectedLength int
	}{
		{
			name:           "version less than versionLast uses deprecated commands",
			version:        AerospikeVersionSupportsSIndexContext,
			expectedLength: commandsNumber,
			expectedCmds: map[int]string{
				cmdIDBuild:               cmdBuild,
				cmdIDSetsOfNamespace:     cmdSetsOfNamespace,
				cmdIDNamespaceInfo:       cmdNamespaceInfo,
				cmdIDRack:                cmdRack,
				cmdIDServiceClearStd:     cmdServiceClearStd,
				cmdIDServiceTLSStd:       cmdServiceTLSStd,
				cmdIDSindexList:          cmdSindexListDeprecated, // deprecated version
				cmdIDUdfList:             cmdUdfList,
				cmdIDUdfGetFilename:      cmdUdfGetFilename,
				cmdIDCreateXDRDC:         cmdCreateXDRDC,
				cmdIDCreateConnector:     cmdCreateConnector,
				cmdIDCreateXDRNode:       cmdCreateXDRNode,
				cmdIDCreateXDRNamespace:  cmdCreateXDRNamespace,
				cmdIDDeleteXDRDC:         cmdDeleteXDRDC,
				cmdIDGetXDRStats:         cmdGetXDRStats,
				cmdIDBlockMRTWrites:      cmdBlockMRTWrites,
				cmdIDUnBlockMRTWrites:    cmdUnBlockMRTWrites,
				cmdIDSetXDRMaxThroughput: cmdSetXDRMaxThroughput,
				cmdIDSetXDRForward:       cmdSetXDRForward,
			},
		},
		{
			name:           "version greater or equal to versionLast uses new commands",
			version:        AerospikeVersionRecentInfoCommands,
			expectedLength: commandsNumber,
			expectedCmds: map[int]string{
				cmdIDBuild:               cmdBuild,
				cmdIDSetsOfNamespace:     cmdSetsOfNamespace,
				cmdIDNamespaceInfo:       cmdNamespaceInfo,
				cmdIDRack:                cmdRack,
				cmdIDServiceClearStd:     cmdServiceClearStd,
				cmdIDServiceTLSStd:       cmdServiceTLSStd,
				cmdIDSindexList:          cmdSindexList, // new version
				cmdIDUdfList:             cmdUdfList,
				cmdIDUdfGetFilename:      cmdUdfGetFilename,
				cmdIDCreateXDRDC:         cmdCreateXDRDC,
				cmdIDCreateConnector:     cmdCreateConnector,
				cmdIDCreateXDRNode:       cmdCreateXDRNode,
				cmdIDCreateXDRNamespace:  cmdCreateXDRNamespace,
				cmdIDDeleteXDRDC:         cmdDeleteXDRDC,
				cmdIDGetXDRStats:         cmdGetXDRStats,
				cmdIDBlockMRTWrites:      cmdBlockMRTWrites,
				cmdIDUnBlockMRTWrites:    cmdUnBlockMRTWrites,
				cmdIDSetXDRMaxThroughput: cmdSetXDRMaxThroughput,
				cmdIDSetXDRForward:       cmdSetXDRForward,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := newCmdDict(tt.version)

			// Check result length
			if len(result) != tt.expectedLength {
				t.Errorf("newCmdDict() length = %d, want %d", len(result), tt.expectedLength)
			}

			// Check each command
			for cmdID, expectedCmd := range tt.expectedCmds {
				if actualCmd, exists := result[cmdID]; !exists {
					t.Errorf("newCmdDict() missing command with ID %d", cmdID)
				} else if actualCmd != expectedCmd {
					t.Errorf("newCmdDict() cmdID %d = %q, want %q", cmdID, actualCmd, expectedCmd)
				}
			}

			if !reflect.DeepEqual(result, tt.expectedCmds) {
				t.Errorf("newCmdDict() = %v, want %v", result, tt.expectedCmds)
			}
		})
	}
}
