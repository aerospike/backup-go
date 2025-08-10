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

const (
	cmdIDBuild = iota
	cmdIDStatus
	cmdIDNamespaces
	cmdIDSetsOfNamespace
	cmdIDNamespaceInfo
	cmdIDRack
	cmdIDServiceClearStd
	cmdIDServiceTLSStd
	cmdIDSindexList
	cmdIDUdfList
	cmdIDUdfGetFilename
	cmdIDCreateXDRDC
	cmdIDCreateConnector
	cmdIDCreateXDRNode
	cmdIDCreateXDRNamespace
	cmdIDDeleteXDRDC
	cmdIDGetXDRStats
	cmdIDBlockMRTWrites
	cmdIDUnBlockMRTWrites
	cmdIDSetXDRMaxThroughput
	cmdIDSetXDRForward
	cmdIDGetConfigXDR
)

// commandsNumber shows how many commands we have, if you add new command, increase this number.
const commandsNumber = 19

// Old commands for db version < AerospikeVersionRecentInfoCommands
const (
	// cmdBuild as we need to check version before we form dict, this command will be called directly.
	cmdBuild               = "build"
	cmdStatus              = "status"
	cmdNamespaces          = "namespaces"
	cmdSetsOfNamespace     = "sets/%s"
	cmdNamespaceInfo       = "namespace/%s"
	cmdRack                = "racks:"
	cmdServiceClearStd     = "service-clear-std"
	cmdServiceTLSStd       = "service-tls-std"
	cmdUdfList             = "udf-list"
	cmdUdfGetFilename      = "udf-get:filename=%s"
	cmdCreateXDRDC         = "set-config:context=xdr;dc=%s;action=create"
	cmdCreateConnector     = "set-config:context=xdr;dc=%s;connector=true"
	cmdCreateXDRNode       = "set-config:context=xdr;dc=%s;node-address-port=%s;action=add"
	cmdCreateXDRNamespace  = "set-config:context=xdr;dc=%s;namespace=%s;action=add;rewind=%s"
	cmdDeleteXDRDC         = "set-config:context=xdr;dc=%s;action=delete"
	cmdGetXDRStats         = "get-stats:context=xdr;dc=%s;namespace=%s"
	cmdBlockMRTWrites      = "set-config:context=namespace;namespace=%s;disable-mrt-writes=true"
	cmdUnBlockMRTWrites    = "set-config:context=namespace;namespace=%s;disable-mrt-writes=false"
	cmdSetXDRMaxThroughput = "set-config:context=xdr;dc=%s;namespace=%s;max-throughput=%d"
	cmdSetXDRForward       = "set-config:context=xdr;dc=%s;namespace=%s;forward=%t"
	cmdGetConfigXDR        = "get-config:context=xdr"

	// Deprecated commands:

	cmdSindexListDeprecated = "sindex-list:ns=%s"
)

// New commands for db version >= AerospikeVersionRecentInfoCommands
const (
	cmdSindexList = "sindex-list:namespace=%s"
)

func newCmdDict(version AerospikeVersion) map[int]string {
	cmds := make(map[int]string, commandsNumber)

	cmds[cmdIDBuild] = cmdBuild
	cmds[cmdIDStatus] = cmdStatus
	cmds[cmdIDNamespaces] = cmdNamespaces
	cmds[cmdIDSetsOfNamespace] = cmdSetsOfNamespace
	cmds[cmdIDNamespaceInfo] = cmdNamespaceInfo
	cmds[cmdIDRack] = cmdRack
	cmds[cmdIDServiceClearStd] = cmdServiceClearStd
	cmds[cmdIDServiceTLSStd] = cmdServiceTLSStd
	cmds[cmdIDSindexList] = cmdSindexListDeprecated
	cmds[cmdIDUdfList] = cmdUdfList
	cmds[cmdIDUdfGetFilename] = cmdUdfGetFilename
	cmds[cmdIDCreateXDRDC] = cmdCreateXDRDC
	cmds[cmdIDCreateConnector] = cmdCreateConnector
	cmds[cmdIDCreateXDRNode] = cmdCreateXDRNode
	cmds[cmdIDCreateXDRNamespace] = cmdCreateXDRNamespace
	cmds[cmdIDDeleteXDRDC] = cmdDeleteXDRDC
	cmds[cmdIDGetXDRStats] = cmdGetXDRStats
	cmds[cmdIDBlockMRTWrites] = cmdBlockMRTWrites
	cmds[cmdIDUnBlockMRTWrites] = cmdUnBlockMRTWrites
	cmds[cmdIDSetXDRMaxThroughput] = cmdSetXDRMaxThroughput
	cmds[cmdIDSetXDRForward] = cmdSetXDRForward
	cmds[cmdIDGetConfigXDR] = cmdGetConfigXDR

	if version.IsGreaterOrEqual(AerospikeVersionRecentInfoCommands) {
		cmds[cmdIDSindexList] = cmdSindexList
	}

	return cmds
}
