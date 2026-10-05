// Copyright 2024-2026 Aerospike, Inc.
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
	"fmt"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// Commands without variable parameters.
const (
	// cmdBuild is called directly, as the version must be known before
	// infoCommands is created.
	cmdBuild           = "build"
	cmdStatus          = "status"
	cmdNamespaces      = "namespaces"
	cmdServiceClearStd = "service-clear-std"
	cmdServiceTLSStd   = "service-tls-std"
	cmdUdfList         = "udf-list"
	cmdStatistics      = "statistics"
	cmdShowJobsQueries = "query-show"
	cmdRacks           = "racks:"
	cmdReplicas        = "replicas:max=1"
)

// Names of commands with parameters.
const (
	cmdNameSetsOfNamespace = "sets"
	cmdNameNamespaceInfo   = "namespace"
	cmdNameUdfGet          = "udf-get"
	cmdNameClusterStable   = "cluster-stable"
	cmdNameSindexList      = "sindex-list"
	cmdNameBackup          = "backup"
	cmdNameBackupStatus    = "backup-status"
	cmdNameBackupAbort     = "backup-abort"
	cmdNameRestorePrepare  = "restore-prepare"
	cmdNameRestore         = "restore"
	cmdNameRestoreStatus   = "restore-status"
	cmdNameRestoreAbort    = "restore-abort"
)

// Command parameter keys.
const (
	paramFilename           = "filename"
	paramSize               = "size"
	paramIgnoreMigrations   = "ignore-migrations"
	paramNamespace          = "namespace"
	paramNamespaceShort     = "ns"
	paramB64                = "b64"
	paramJobID              = "job-id"
	paramObjectStorageType  = "object-storage-type"
	paramS3Bucket           = "s3-bucket"
	paramS3Region           = "s3-region"
	paramS3Profile          = "s3-profile"
	paramAccessKey          = "access-key"
	paramSecretKey          = "secret-key"
	paramS3Endpoint         = "s3-endpoint"
	paramModifiedBefore     = "modified-before"
	paramModifiedAfter      = "modified-after"
	paramSetList            = "set-list"
	paramNoIndexes          = "no-indexes"
	paramNoUDFs             = "no-udfs"
	paramEnableChangeStream = "enable-change-stream"
	paramFuzzyRestore       = "fuzzy-restore"
	paramPath               = "path"
	paramNodes              = "nodes"
)

// infoCommands builds info commands for a specific Aerospike server version.
type infoCommands struct {
	version infomodels.AerospikeVersion
}

func newInfoCommands(v infomodels.AerospikeVersion) infoCommands {
	return infoCommands{version: v}
}

// require returns errCommandNotSupported if the server version is lower than minVersion.
func (c infoCommands) require(name string, minVersion infomodels.AerospikeVersion) error {
	if c.version.IsGreaterOrEqual(minVersion) {
		return nil
	}

	return fmt.Errorf("%w: %s requires %s, server version is %s",
		errCommandNotSupported, name, minVersion, c.version)
}

// requireIntegratedBackup returns errCommandNotSupported if the server version
// is lower than [infomodels.AerospikeVersionSupportsIntegratedBackup].
func (c infoCommands) requireIntegratedBackup(name string) error {
	return c.require(name, infomodels.AerospikeVersionSupportsIntegratedBackup)
}

// Commands available on all supported versions.

// setsOfNamespace returns the command that lists the sets of namespace ns.
func (c infoCommands) setsOfNamespace(ns string) (string, error) {
	return buildPathCmd(cmdNameSetsOfNamespace, paramNamespace, ns)
}

// namespaceInfo returns the command that reads the statistics of namespace ns.
func (c infoCommands) namespaceInfo(ns string) (string, error) {
	return buildPathCmd(cmdNameNamespaceInfo, paramNamespace, ns)
}

// udfGet returns the command that reads the UDF stored as filename.
func (c infoCommands) udfGet(filename string) (string, error) {
	return newInfoCmd(cmdNameUdfGet).
		required(paramFilename, filename).
		build()
}

// clusterStable returns the command that checks that the cluster of the given
// size is stable for namespace ns.
func (c infoCommands) clusterStable(size int, ns string) (string, error) {
	return newInfoCmd(cmdNameClusterStable).
		num(paramSize, size).
		flag(paramIgnoreMigrations, false).
		required(paramNamespace, ns).
		build()
}

// validateClusterStable checks everything clusterStable needs except the
// cluster size, so that a call that can never succeed is rejected before the
// size is read.
func (c infoCommands) validateClusterStable(ns string) error {
	_, err := newInfoCmd(cmdNameClusterStable).
		required(paramNamespace, ns).
		build()

	return err
}

// sindexList returns the command that lists the secondary indexes of namespace ns.
// Before Aerospike 8.1 the namespace is passed as "ns", since 8.1 as "namespace".
// withCtx requests the index context in base64; the caller decides whether the
// server supports it.
func (c infoCommands) sindexList(ns string, withCtx bool) (string, error) {
	nsKey := paramNamespaceShort
	if c.version.IsGreaterOrEqual(infomodels.AerospikeVersionRecentInfoCommands) {
		nsKey = paramNamespace
	}

	cmd := newInfoCmd(cmdNameSindexList).required(nsKey, ns)
	if withCtx {
		cmd.flag(paramB64, true)
	}

	return cmd.build()
}

// Commands that require Aerospike >= 8.1 (integrated backup).

// serverBackup returns the command that starts the backup job jobID.
func (c infoCommands) serverBackup(r *infomodels.RequestBackup, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameBackup); err != nil {
		return "", err
	}

	if r == nil {
		return "", fmt.Errorf("%w: %s", errNilRequest, cmdNameBackup)
	}

	return newInfoCmd(cmdNameBackup).
		required(paramNamespace, r.Namespace).
		required(paramJobID, jobID).
		required(paramObjectStorageType, r.Storage).
		str(paramS3Bucket, r.Bucket).
		str(paramS3Region, r.Region).
		str(paramS3Profile, r.Profile).
		str(paramAccessKey, r.AccessKey).
		str(paramSecretKey, r.SecretKey).
		str(paramS3Endpoint, r.Endpoint).
		str(paramModifiedBefore, r.ModifiedBefore).
		str(paramModifiedAfter, r.ModifiedAfter).
		str(paramSetList, r.SetList).
		flag(paramNoIndexes, r.NoIndexes).
		flag(paramNoUDFs, r.NoUDFs).
		flag(paramEnableChangeStream, r.EnableChangeStream).
		build()
}

// serverRestore returns the command that starts the restore job r.JobID.
func (c infoCommands) serverRestore(r *infomodels.RequestRestore) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestore); err != nil {
		return "", err
	}

	if r == nil {
		return "", fmt.Errorf("%w: %s", errNilRequest, cmdNameRestore)
	}

	return newInfoCmd(cmdNameRestore).
		required(paramNamespace, r.Namespace).
		required(paramJobID, r.JobID).
		required(paramObjectStorageType, r.Storage).
		str(paramS3Bucket, r.Bucket).
		str(paramS3Region, r.Region).
		str(paramS3Profile, r.Profile).
		str(paramAccessKey, r.AccessKey).
		str(paramSecretKey, r.SecretKey).
		str(paramS3Endpoint, r.Endpoint).
		flag(paramFuzzyRestore, r.FuzzyRestore).
		str(paramPath, r.Path).
		build()
}

// validatePrepareRestore checks everything serverPrepareRestore needs except the
// node list, so that a call that can never succeed is rejected before the node
// list is read.
func (c infoCommands) validatePrepareRestore(ns, jobID string) error {
	if err := c.requireIntegratedBackup(cmdNameRestorePrepare); err != nil {
		return err
	}

	_, err := prepareRestoreCmd(ns, jobID).build()

	return err
}

// serverPrepareRestore returns the command that prepares namespace ns on the
// given comma-separated nodes for the restore job jobID.
func (c infoCommands) serverPrepareRestore(ns, jobID, nodes string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestorePrepare); err != nil {
		return "", err
	}

	return prepareRestoreCmd(ns, jobID).
		required(paramNodes, nodes).
		build()
}

// backupStatus returns the command that reads the state of the backup job jobID.
func (c infoCommands) backupStatus(jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameBackupStatus); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameBackupStatus).
		required(paramJobID, jobID).
		build()
}

// restoreStatus returns the command that reads the restore state of namespace ns.
func (c infoCommands) restoreStatus(ns string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreStatus); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreStatus).
		required(paramNamespace, ns).
		build()
}

// backupAbort returns the command that aborts the backup job jobID.
func (c infoCommands) backupAbort(jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameBackupAbort); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameBackupAbort).
		required(paramJobID, jobID).
		build()
}

// restoreAbort returns the command that aborts the restore job jobID of namespace ns.
func (c infoCommands) restoreAbort(ns, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreAbort); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreAbort).
		required(paramNamespace, ns).
		required(paramJobID, jobID).
		build()
}

// prepareRestoreCmd returns the restore-prepare command without the node list.
func prepareRestoreCmd(ns, jobID string) *infoCmd {
	return newInfoCmd(cmdNameRestorePrepare).
		required(paramNamespace, ns).
		required(paramJobID, jobID)
}
