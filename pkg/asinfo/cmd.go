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
	cmdNameRestorePrepare      = "restore-prepare"
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
	paramNodes                  = "nodes"
	paramHydrateReplica         = "hydrate-replica"
	paramAllowUnhosted          = "allow-unhosted"
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
		required(paramJobID, jobID)

	addStorageParams(cmd, &r.RequestCommon)

	cmd.str(paramSetList, r.SetList).
		str(paramBinList, r.BinList).
		str(paramModifiedAfter, r.ModifiedAfter).
		str(paramModifiedBefore, r.ModifiedBefore).
		str(paramFilterExp, r.FilterExp).
		optFlag(paramNoIndexes, r.NoIndexes).
		optFlag(paramNoUDFs, r.NoUDFs).
		optNum(paramBackupRPS, r.BackupRPS).
		optNum(paramBackupWorkerThreads, r.BackupWorkerThreads).
		optNum(paramBackupUploadThreads, r.BackupUploadThreads).
		optNum(paramBackupS3Throughput, r.BackupS3ThroughputTargetGbps).
		str(paramBackupPolicy, string(r.Policy))

	return buildChecked(cmd,
		checkPolicy(cmdNameBackup, paramBackupPolicy, r.Policy, knownBackupPolicy))
}

// serverRestore returns the command that starts the restore job r.JobID from
// the backups r.BackupIDs with the policy r.Policy. A parameter that the policy
// does not accept fails with errParamNotApplicable, see checkRestoreMode.
func (c infoCommands) serverRestore(r *infomodels.RequestRestore) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestore); err != nil {
		return "", err
	}

	if r == nil {
		return "", fmt.Errorf("%w: %s", errNilRequest, cmdNameRestore)
	}

	cmd := newInfoCmd(cmdNameRestore, jobUnsafeChars).
		required(paramNamespace, r.Namespace).
		required(paramJobID, r.JobID).
		requiredCommaList(paramBackupIDs, r.BackupIDs)

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

	return buildChecked(cmd, checkRestoreMode(r))
}

// addFuzzyOnlyParams adds the parameters of r that only the fuzzy policy
// accepts. checkRestoreMode finds the ones r sets with it, so every fuzzy only
// parameter must be added here.
func addFuzzyOnlyParams(cmd *infoCmd, r *infomodels.RequestRestore) *infoCmd {
	return cmd.optFlag(paramAllowUnhosted, r.AllowUnhosted).
		optNum(paramMaxInflight, r.MaxInflight).
		optNumPtr(paramRetryBaseInterval, r.RetryBaseIntervalMs).
		optFloat(paramRetryMultiplier, r.RetryMultiplier).
		optNum(paramRetryMaxAttempts, r.RetryMaxAttempts).
		optFlag(paramIgnoreRecordError, r.IgnoreRecordError).
		optFlag(paramApplyMetadataLast, r.ApplyMetadataLast)
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
// accept: see checkPolicyParams and checkIncrementalPath.
func checkRestoreMode(r *infomodels.RequestRestore) error {
	err := checkPolicy(cmdNameRestore, paramRestorePolicy, r.Policy, knownRestorePolicy)
	if err != nil {
		return err
	}

	return joinErrs(checkPolicyParams(r), checkIncrementalPath(r))
}

// checkPolicyParams returns errParamNotApplicable if r sets a parameter that
// r.Policy does not accept: a strict only parameter for the fuzzy policy, see
// strictOnlyParams, or a fuzzy only parameter for the strict one. An empty
// policy is checked as strict, the server default.
func checkPolicyParams(r *infomodels.RequestRestore) error {
	if r.Policy == infomodels.RestorePolicyFuzzy {
		if strictOnly := strictOnlyParams(r); len(strictOnly) > 0 {
			return notApplicableErr(r.Policy, strictOnly)
		}

		return nil
	}

	fuzzyOnly := addFuzzyOnlyParams(newInfoCmd(cmdNameRestore), r).paramKeys()
	if len(fuzzyOnly) > 0 {
		return notApplicableErr(r.Policy, fuzzyOnly)
	}

	return nil
}

// strictOnlyParams returns the keys of the parameters set in r that only the
// strict policy accepts, in the order the command sends them. The fuzzy policy
// restores a single backup, so it rejects more than one backup id.
func strictOnlyParams(r *infomodels.RequestRestore) []string {
	var keys []string

	if backupIDCount(r.BackupIDs) > 1 {
		keys = append(keys, paramBackupIDs)
	}

	if r.FilterExp != "" {
		keys = append(keys, paramFilterExp)
	}

	return keys
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

// validatePrepareRestore checks everything serverPrepareRestore needs except the
// node list, so that a call that can never succeed is rejected before the node
// list is read.
func (c infoCommands) validatePrepareRestore(r *infomodels.RequestPrepareRestore) error {
	if err := c.checkPrepareRestore(r); err != nil {
		return err
	}

	_, err := prepareRestoreCmd(r).build()

	return err
}

// serverPrepareRestore returns the command that prepares namespace r.Namespace
// on the given comma-separated nodes for the restore job r.JobID.
func (c infoCommands) serverPrepareRestore(r *infomodels.RequestPrepareRestore, nodes string) (string, error) {
	if err := c.checkPrepareRestore(r); err != nil {
		return "", err
	}

	return prepareRestoreCmd(r).
		required(paramNodes, nodes).
		build()
}

// backupStatus returns the command that reads the state of the backup job jobID.
func (c infoCommands) backupStatus(jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameBackupStatus); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameBackupStatus, jobUnsafeChars).
		required(paramJobID, jobID).
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
		required(paramJobID, jobID).
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
		required(paramJobID, jobID).
		build()
}

// restoreAbort returns the command that aborts the restore job jobID of namespace ns.
func (c infoCommands) restoreAbort(ns, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreAbort); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreAbort, jobUnsafeChars).
		required(paramNamespace, ns).
		required(paramJobID, jobID).
		build()
}

// restoreOpen returns the command that releases namespace ns held by the
// aborted cold restore job jobID.
func (c infoCommands) restoreOpen(ns, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreOpen); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreOpen, jobUnsafeChars).
		required(paramNamespace, ns).
		required(paramJobID, jobID).
		build()
}

// restoreClearStorage returns the command that clears the storage of namespace
// ns on the node it is sent to, for the cold restore job jobID being prepared.
func (c infoCommands) restoreClearStorage(ns, jobID string) (string, error) {
	if err := c.requireIntegratedBackup(cmdNameRestoreClearStorage); err != nil {
		return "", err
	}

	return newInfoCmd(cmdNameRestoreClearStorage, jobUnsafeChars).
		required(paramNamespace, ns).
		required(paramJobID, jobID).
		build()
}

// checkPrepareRestore checks that the server supports the restore preparation
// and that the request is set.
func (c infoCommands) checkPrepareRestore(r *infomodels.RequestPrepareRestore) error {
	if err := c.requireIntegratedBackup(cmdNameRestorePrepare); err != nil {
		return err
	}

	if r == nil {
		return fmt.Errorf("%w: %s", errNilRequest, cmdNameRestorePrepare)
	}

	return nil
}

// prepareRestoreCmd returns the restore-prepare command without the node list.
func prepareRestoreCmd(r *infomodels.RequestPrepareRestore) *infoCmd {
	return newInfoCmd(cmdNameRestorePrepare, jobUnsafeChars).
		required(paramNamespace, r.Namespace).
		required(paramJobID, r.JobID).
		optFlag(paramHydrateReplica, r.HydrateReplica)
}

// addStorageParams adds the object storage parameters shared by the backup and
// restore commands.
func addStorageParams(cmd *infoCmd, r *infomodels.RequestCommon) {
	cmd.required(paramObjectStorageType, r.Storage).
		str(paramPath, r.Path).
		str(paramS3Bucket, r.Bucket).
		str(paramS3Region, r.Region).
		str(paramS3Profile, r.Profile).
		str(paramS3Endpoint, r.Endpoint)
}
