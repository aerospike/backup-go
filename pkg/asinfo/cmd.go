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
	"strings"

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
	cmdNameSetsOfNamespace     = "sets"
	cmdNameNamespaceInfo       = "namespace"
	cmdNameUdfGet              = "udf-get"
	cmdNameClusterStable       = "cluster-stable"
	cmdNameSindexList          = "sindex-list"
	cmdNameBackup              = "backup"
	cmdNameBackupStatus        = "backup-status"
	cmdNameBackupAbort         = "backup-abort"
	cmdNameRestore             = "restore"
	cmdNameRestoreStatus       = "restore-status"
	cmdNameRestoreAbort        = "restore-abort"
	cmdNameRestoreOpen         = "restore-open"
	cmdNameRestoreClearStorage = "restore-clear-storage"
)

// Command parameter keys.
const (
	paramFilename               = "filename"
	paramSize                   = "size"
	paramIgnoreMigrations       = "ignore-migrations"
	paramNamespace              = "namespace"
	paramNamespaceShort         = "ns"
	paramB64                    = "b64"
	paramJobID                  = "job-id"
	paramBackupIDs              = "backup-ids"
	paramObjectStorageType      = "object-storage-type"
	paramPath                   = "path"
	paramS3Bucket               = "s3-bucket"
	paramS3Region               = "s3-region"
	paramS3Profile              = "s3-profile"
	paramS3Endpoint             = "s3-endpoint"
	paramSetList                = "set-list"
	paramBinList                = "bin-list"
	paramModifiedAfter          = "modified-after"
	paramModifiedBefore         = "modified-before"
	paramFilterExp              = "filter-exp"
	paramNoIndexes              = "no-indexes"
	paramNoUDFs                 = "no-udfs"
	paramNoConfig               = "no-config"
	paramHydrateReplica         = "hydrate-replica"
	paramMaxInflight            = "max-inflight"
	paramRetryBaseInterval      = "retry-base-interval"
	paramRetryMultiplier        = "retry-multiplier"
	paramRetryMaxAttempts       = "retry-max-attempts"
	paramIgnoreRecordError      = "ignore-record-error"
	paramApplyMetadataLast      = "apply-metadata-last"
	paramIncrementalPath        = "incremental-path"
	paramRestoreDownloadThreads = "restore-download-threads"
	paramRestoreWorkerThreads   = "restore-worker-threads"
	paramRestoreRPS             = "restore-rps"
	paramExtraTTL               = "extra-ttl"
	paramBackupRPS              = "backup-rps"
	paramBackupWorkerThreads    = "backup-worker-threads"
	paramBackupUploadThreads    = "backup-upload-threads"
	paramBackupS3Throughput     = "backup-s3-throughput-target-gbps"
	paramBackupPolicy           = "backup-policy"
	paramRestorePolicy          = "restore-policy"
)

// jobUnsafeChars must not appear in a value of a backup or restore command, as
// the server uses "|" as a separator of the values it stores with the job and
// rejects it in every job id.
const jobUnsafeChars = "|"

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

	cmd := newInfoCmd(cmdNameBackup, jobUnsafeChars).
		required(paramNamespace, r.Namespace).
		jobID(paramJobID, jobID)

	addStorageParams(cmd, &r.RequestCommon)

	cmd.str(paramSetList, r.SetList).
		str(paramBinList, r.BinList).
		str(paramModifiedAfter, r.ModifiedAfter).
		str(paramModifiedBefore, r.ModifiedBefore).
		str(paramFilterExp, r.FilterExp).
		optFlag(paramNoIndexes, r.NoIndexes).
		optFlag(paramNoUDFs, r.NoUDFs).
		optFlag(paramNoConfig, r.NoConfig).
		optNum(paramBackupRPS, r.BackupRPS).
		optNum(paramBackupWorkerThreads, r.BackupWorkerThreads).
		optNum(paramBackupUploadThreads, r.BackupUploadThreads).
		optNum(paramBackupS3Throughput, r.BackupS3ThroughputTargetGbps).
		str(paramBackupPolicy, string(r.Policy))

	return buildChecked(cmd, joinErrs(
		checkPolicy(cmdNameBackup, paramBackupPolicy, r.Policy, knownBackupPolicy),
		checkBackupStorage(r)))
}

// checkBackupStorage returns errParamNotApplicable if r asks for a policy that
// may commit a fuzzy backup, best-effort or fuzzy, on a file system storage.
// An empty policy is accepted: the server default is strict.
func checkBackupStorage(r *infomodels.RequestBackup) error {
	if !isFileSystemStorage(r.Storage) ||
		(r.Policy != infomodels.BackupPolicyBestEffort && r.Policy != infomodels.BackupPolicyFuzzy) {
		return nil
	}

	return storageNotApplicableErr(cmdNameBackup, paramBackupPolicy, r.Storage)
}

// isFileSystemStorage reports whether storage is a file system storage type,
// which only a strict backup and a cold restore accept: a fuzzy backup or
// restore needs an object store.
func isFileSystemStorage(storage infomodels.StorageType) bool {
	return storage == infomodels.StorageTypeFS || storage == infomodels.StorageTypeFA
}

// storageNotApplicableErr reports that the command cmdName does not accept the
// policy it sends as policyKey on the file system storage storage.
func storageNotApplicableErr(cmdName, policyKey string, storage infomodels.StorageType) error {
	return paramsErr(errParamNotApplicable,
		cmdName+" with "+paramObjectStorageType+"="+string(storage), []string{policyKey})
}

// serverRestore returns the command that starts the restore job jobID from
// the backups r.BackupIDs with the policy r.Policy. A parameter that the policy
// does not accept fails with errParamNotApplicable, see checkRestoreMode.
func (c infoCommands) serverRestore(r *infomodels.RequestRestore, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestore); err != nil {
		return "", err
	}

	if r == nil {
		return "", fmt.Errorf("%w: %s", errNilRequest, cmdNameRestore)
	}

	cmd := newInfoCmd(cmdNameRestore, jobUnsafeChars).
		required(paramNamespace, r.Namespace).
		jobID(paramJobID, jobID).
		requiredIDList(paramBackupIDs, r.BackupIDs)

	addStorageParams(cmd, &r.RequestCommon)

	// The command is returned only if checkRestoreMode accepts every parameter
	// for the policy, so the parameters of both policies are added unconditionally.
	cmd.str(paramIncrementalPath, r.IncrementalPath).
		optFlag(paramNoIndexes, r.NoIndexes).
		optFlag(paramNoUDFs, r.NoUDFs).
		str(paramSetList, r.SetList).
		str(paramFilterExp, r.FilterExp).
		optNum(paramRestoreDownloadThreads, r.RestoreDownloadThreads).
		optNum(paramRestoreWorkerThreads, r.RestoreWorkerThreads).
		optNum(paramRestoreRPS, r.RestoreRPS).
		optNum(paramExtraTTL, r.ExtraTTL).
		str(paramRestorePolicy, string(r.Policy))

	addFuzzyOnlyParams(cmd, r)
	addColdOnlyParams(cmd, r)

	return buildChecked(cmd, checkRestoreMode(r))
}

// addFuzzyOnlyParams adds the parameters of r that only the fuzzy policy
// accepts. checkRestoreMode finds the ones r sets with it, so every fuzzy only
// parameter must be added here.
func addFuzzyOnlyParams(cmd *infoCmd, r *infomodels.RequestRestore) *infoCmd {
	return cmd.optNum(paramMaxInflight, r.MaxInflight).
		optNumPtr(paramRetryBaseInterval, r.RetryBaseIntervalMs).
		optFloat(paramRetryMultiplier, r.RetryMultiplier).
		optNumPtr(paramRetryMaxAttempts, r.RetryMaxAttempts).
		optFlag(paramIgnoreRecordError, r.IgnoreRecordError).
		optFlag(paramApplyMetadataLast, r.ApplyMetadataLast)
}

// addColdOnlyParams adds the parameters of r that only the strict (cold)
// policy accepts. checkRestoreMode finds the ones r sets with another policy,
// so every cold only parameter must be added here.
func addColdOnlyParams(cmd *infoCmd, r *infomodels.RequestRestore) *infoCmd {
	return cmd.optFlag(paramHydrateReplica, r.HydrateReplica)
}

// knownBackupPolicy reports whether the server accepts the backup policy p.
func knownBackupPolicy(p infomodels.BackupPolicy) bool {
	switch p {
	case infomodels.BackupPolicyStrict,
		infomodels.BackupPolicyBestEffort,
		infomodels.BackupPolicyFuzzy:
		return true
	default:
		return false
	}
}

// knownRestorePolicy reports whether the server accepts the restore policy p.
func knownRestorePolicy(p infomodels.RestorePolicy) bool {
	switch p {
	case infomodels.RestorePolicyStrict, infomodels.RestorePolicyFuzzy:
		return true
	default:
		return false
	}
}

// checkPolicy returns errUnknownPolicy if policy is set and known rejects it.
// An empty policy is not sent, so the server applies its default. Unlike the
// other errors of the commands, the error holds the value: a policy is never a
// secret, and the value tells which policy name is wrong.
func checkPolicy[P ~string](cmdName, key string, policy P, known func(P) bool) error {
	if policy == "" || known(policy) {
		return nil
	}

	return fmt.Errorf("%w: %s: %s=%q", errUnknownPolicy, cmdName, key, policy)
}

// checkRestoreMode returns errUnknownPolicy if r.Policy is unknown, and
// errParamNotApplicable if r sets a parameter that the rest of r does not
// accept: see checkPolicyParams, checkIncrementalPath, checkRestoreStorage and
// checkMetadataApply.
func checkRestoreMode(r *infomodels.RequestRestore) error {
	err := checkPolicy(cmdNameRestore, paramRestorePolicy, r.Policy, knownRestorePolicy)
	if err != nil {
		return err
	}

	return joinErrs(checkPolicyParams(r), checkIncrementalPath(r), checkRestoreStorage(r),
		checkMetadataApply(r))
}

// checkRestoreStorage returns errParamNotApplicable if r asks for the fuzzy
// policy on a file system storage.
func checkRestoreStorage(r *infomodels.RequestRestore) error {
	if r.Policy != infomodels.RestorePolicyFuzzy || !isFileSystemStorage(r.Storage) {
		return nil
	}

	return storageNotApplicableErr(cmdNameRestore, paramRestorePolicy, r.Storage)
}

// checkMetadataApply returns errParamNotApplicable if r sets ApplyMetadataLast,
// whatever its value, while skipping both the secondary indexes and the UDFs:
// nothing is left to apply. It checks the fuzzy policy only, since the other
// policies reject ApplyMetadataLast anyway, see checkPolicyParams.
func checkMetadataApply(r *infomodels.RequestRestore) error {
	if r.Policy != infomodels.RestorePolicyFuzzy || r.ApplyMetadataLast == nil ||
		!isTrue(r.NoIndexes) || !isTrue(r.NoUDFs) {
		return nil
	}

	return paramsErr(errParamNotApplicable,
		cmdNameRestore+" with "+paramNoIndexes+"=true and "+paramNoUDFs+"=true",
		[]string{paramApplyMetadataLast})
}

// isTrue reports whether the optional flag b is set to true.
func isTrue(b *bool) bool {
	return b != nil && *b
}

// checkPolicyParams returns errParamNotApplicable if r sets a fuzzy only
// parameter for a policy other than fuzzy, or a cold only parameter for the
// fuzzy policy. An empty policy is checked as strict, the server default.
func checkPolicyParams(r *infomodels.RequestRestore) error {
	var rejected []string

	if r.Policy == infomodels.RestorePolicyFuzzy {
		rejected = addColdOnlyParams(newInfoCmd(cmdNameRestore), r).paramKeys()
	} else {
		rejected = addFuzzyOnlyParams(newInfoCmd(cmdNameRestore), r).paramKeys()
	}

	if len(rejected) > 0 {
		return notApplicableErr(r.Policy, rejected)
	}

	return nil
}

// checkIncrementalPath returns errParamNotApplicable if r sets an incremental
// path for a single backup id, with any policy: the path holds the backups
// that follow the first one.
func checkIncrementalPath(r *infomodels.RequestRestore) error {
	if r.IncrementalPath == "" || backupIDCount(r.BackupIDs) != 1 {
		return nil
	}

	return paramsErr(errParamNotApplicable, cmdNameRestore+" with one backup id",
		[]string{paramIncrementalPath})
}

// backupIDCount returns the number of ids in the comma-separated list ids, or
// 0 if the list is empty or invalid. The builder reports such a list, so the
// checks that count the ids skip it instead of reporting it a second time.
func backupIDCount(ids string) int {
	if !commaSeparatedListValid(ids) {
		return 0
	}

	return strings.Count(ids, ",") + 1
}

// notApplicableErr reports the keys of the parameters that the restore policy
// does not accept. An empty policy is reported as the default strict one.
func notApplicableErr(policy infomodels.RestorePolicy, keys []string) error {
	mode := paramRestorePolicy + "=" + string(policy)
	if policy == "" {
		mode = "default " + paramRestorePolicy + " (" + string(infomodels.RestorePolicyStrict) + ")"
	}

	return paramsErr(errParamNotApplicable, cmdNameRestore+" with "+mode, keys)
}

// buildChecked returns the command built by cmd, or the error of the build
// joined with checkErr, the result of the checks the builder does not do.
func buildChecked(cmd *infoCmd, checkErr error) (string, error) {
	built, err := cmd.build()
	if err := joinErrs(err, checkErr); err != nil {
		return "", err
	}

	return built, nil
}

// joinErrs returns the errors of errs that are not nil joined into one line,
// or nil if there are none. Unlike errors.Join, it keeps the error on a single
// line, as the other errors of the commands are.
func joinErrs(errs ...error) error {
	var joined error

	for _, err := range errs {
		switch {
		case err == nil:
		case joined == nil:
			joined = err
		default:
			joined = fmt.Errorf("%w; %w", joined, err)
		}
	}

	return joined
}

// backupStatus returns the command that reads the state of the backup job jobID.
func (c infoCommands) backupStatus(jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameBackupStatus); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameBackupStatus, jobUnsafeChars).
		jobID(paramJobID, jobID).
		build()
}

// restoreStatus returns the command that reads the state of the restore job
// jobID. The job is filtered by its id only: job ids are single-use, and a
// namespace filter may be answered with an error by a node that does not know
// the namespace, instead of the row of the job.
func (c infoCommands) restoreStatus(jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreStatus); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreStatus, jobUnsafeChars).
		jobID(paramJobID, jobID).
		build()
}

// validateRestoreStatusNamespace checks the namespace the restore-status rows
// are filtered by on the client side, as restoreStatus does not send it.
func (c infoCommands) validateRestoreStatusNamespace(ns string) error {
	_, err := newInfoCmd(cmdNameRestoreStatus, jobUnsafeChars).
		required(paramNamespace, ns).
		build()

	return err
}

// backupAbort returns the command that aborts the backup job jobID.
func (c infoCommands) backupAbort(jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameBackupAbort); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameBackupAbort, jobUnsafeChars).
		jobID(paramJobID, jobID).
		build()
}

// restoreAbort returns the command that aborts the restore job jobID of namespace ns.
func (c infoCommands) restoreAbort(ns, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreAbort); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreAbort, jobUnsafeChars).
		required(paramNamespace, ns).
		jobID(paramJobID, jobID).
		build()
}

// restoreOpen returns the command that releases namespace ns held by a failed
// or aborted cold restore. A namespace has at most one restore job, so the
// server finds the job by the namespace.
func (c infoCommands) restoreOpen(ns string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreOpen); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreOpen, jobUnsafeChars).
		required(paramNamespace, ns).
		build()
}

// restoreClearStorage returns the command that clears the storage of namespace
// ns on the node it is sent to, while the node prepares the namespace for a
// cold restore.
func (c infoCommands) restoreClearStorage(ns string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreClearStorage); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreClearStorage, jobUnsafeChars).
		required(paramNamespace, ns).
		build()
}

// addStorageParams adds the object storage parameters shared by the backup and
// restore commands.
func addStorageParams(cmd *infoCmd, r *infomodels.RequestCommon) {
	cmd.required(paramObjectStorageType, string(r.Storage)).
		str(paramPath, r.Path).
		str(paramS3Bucket, r.Bucket).
		str(paramS3Region, r.Region).
		str(paramS3Profile, r.Profile).
		str(paramS3Endpoint, r.Endpoint)
}
