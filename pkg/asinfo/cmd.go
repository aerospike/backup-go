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

import "github.com/aerospike/backup-go/pkg/asinfo/models"

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
	cmdIDReplicas
	cmdIDServerBackup
	cmdIDServerRestore
	cmdIDServerPrepareRestore
	cmdIDShowJobsQueries
	cmdIDClusterStable
	cmdIDStatistics
	cmdIDRestoreStatus
	cmdIDBackupStatus
)

// commandsNumber shows how many commands we have, if you add new command, increase this number.
const commandsNumber = 20

// Old commands for db version < AerospikeVersionRecentInfoCommands
const (
	// cmdBuild as we need to check version before we form dict, this command will be called directly.
	cmdBuild           = "build"
	cmdStatus          = "status"
	cmdNamespaces      = "namespaces"
	cmdSetsOfNamespace = "sets/%s"
	cmdNamespaceInfo   = "namespace/%s"
	cmdRack            = "racks:"
	cmdServiceClearStd = "service-clear-std"
	cmdServiceTLSStd   = "service-tls-std"
	cmdUdfList         = "udf-list"
	cmdUdfGetFilename  = "udf-get:filename=%s"
	cmdReplicas        = "replicas:max=1"
	cmdClusterStable   = "cluster-stable:size=%d;ignore-migrations=false;namespace=%s"
	cmdStatistics      = "statistics"

	cmdServerBackup = "backup:namespace=%s;job-id=%s;object_storage_type=%s;s3-bucket=%s;" +
		"s3-region=%s;s3-profile=%s;access-key=%s;secret-key=%s;s3-endpoint=%s;" +
		"modified-before=%s;modified-after=%s;set=%s;no-indexes=%t;no-udfs=%t;enable-change-stream=%t"
	cmdServerRestore = "restore:namespace=%s;job-id=%s;object_storage_type=%s;s3-bucket=%s;" +
		"s3-region=%s;s3-profile=%s;access-key=%s;secret-key=%s;s3-endpoint=%s;fuzzy-restore=%t;path=%s"
	cmdServerPrepareRestore = "prepare-restore:namespace=%s;job-id=%s;nodes=%s"

	cmdShowJobsQueries = "query-show"

	cmdRestoreStatus = "restore-status:namespace=%s;"
	cmdBackupStatus  = "backup-status:job-id=%s;"

	// Deprecated commands:

	cmdSindexListDeprecated = "sindex-list:ns=%s"
)

// New commands for db version >= AerospikeVersionRecentInfoCommands
const (
	cmdSindexList = "sindex-list:namespace=%s"
)

func newCmdDict(version models.AerospikeVersion) map[int]string {
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
	cmds[cmdIDReplicas] = cmdReplicas
	cmds[cmdIDShowJobsQueries] = cmdShowJobsQueries
	cmds[cmdIDClusterStable] = cmdClusterStable
	cmds[cmdIDStatistics] = cmdStatistics

	if version.IsGreaterOrEqual(models.AerospikeVersionRecentInfoCommands) {
		cmds[cmdIDSindexList] = cmdSindexList
	}

	if version.IsGreaterOrEqual(models.AerospikeVersionSupportsIntegratedBackup) {
		cmds[cmdIDServerBackup] = cmdServerBackup
		cmds[cmdIDServerRestore] = cmdServerRestore
		cmds[cmdIDServerPrepareRestore] = cmdServerPrepareRestore
		cmds[cmdIDRestoreStatus] = cmdRestoreStatus
		cmds[cmdIDBackupStatus] = cmdBackupStatus
	}

	return cmds
}
