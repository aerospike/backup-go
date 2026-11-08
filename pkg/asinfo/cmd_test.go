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
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/aerospike/backup-go/errclass"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared by the infoCommands tests below.
const (
	testCmdNamespace = "source-ns"
	testCmdJobID     = "260922T103653-5xsr"
	testOtherJobID   = "260922T090000-abcd"
	testCmdBackupID  = "260901T000000-abcd"
	testCmdStorage   = "aws-s3"
	testCmdStorageFS = "fs"
	testCmdStorageFA = "fa"

	// testBackupOnStorage and testRestoreOnStorage start the error of a policy
	// the storage type does not accept; the storage type follows.
	testBackupOnStorage  = "backup with " + paramObjectStorageType + "="
	testRestoreOnStorage = "restore with " + paramObjectStorageType + "="

	testCmdBucket    = "backup-bucket"
	testCmdRegion    = "eu-central-1"
	testCmdProfile   = "default"
	testCmdEndpoint  = "https://s3.example.com"
	testCmdPath      = "/backups/source-ns"
	testCmdSetList   = "set1,set2"
	testCmdFilterExp = "kwGVfwIAAJMEDqNiaW4="

	testCmdBackupBase  = "backup:namespace=source-ns;job-id=260922T103653-5xsr;object-storage-type=aws-s3"
	testCmdRestoreBase = "restore:namespace=source-ns;job-id=260922T103653-5xsr;" +
		"backup-ids=260901T000000-abcd;object-storage-type=aws-s3"
	testCmdStorageParams = ";path=/backups/source-ns;s3-bucket=backup-bucket;s3-region=eu-central-1;" +
		"s3-profile=default;s3-endpoint=https://s3.example.com"
	testCmdSetListParam   = ";set-list=set1,set2"
	testCmdFilterExpParam = ";filter-exp=kwGVfwIAAJMEDqNiaW4="

	// testCmdBackupIDsChain is a full backup followed by an incremental one.
	testCmdBackupIDsChain = testCmdBackupID + ",260901T000001-efgh"

	testCmdRestorePolicyStrict = ";restore-policy=strict"
	testCmdRestorePolicyFuzzy  = ";restore-policy=fuzzy"

	testNameEmpty   = "empty"
	testNameStrict  = "strict"
	testNameFuzzy   = "fuzzy"
	testNameUnknown = "unknown"
	// testPolicyCold is a policy neither the backup nor the restore knows.
	testPolicyCold = "cold"

	testNameUnsetOmitted = "unset fields are omitted"

	// testCmdMissingNsAndJobID is how the errors report a missing namespace and job id.
	testCmdMissingNsAndJobID = "namespace, job-id"
)

// testStorage returns request fields with every storage parameter set.
func testStorage() infomodels.RequestCommon {
	return infomodels.RequestCommon{
		Namespace: testCmdNamespace,
		Storage:   testCmdStorage,
		Path:      testCmdPath,
		Bucket:    testCmdBucket,
		Region:    testCmdRegion,
		Profile:   testCmdProfile,
		Endpoint:  testCmdEndpoint,
	}
}

// testCmdMinimalCommon returns request fields with only the required ones set.
func testCmdMinimalCommon() infomodels.RequestCommon {
	return infomodels.RequestCommon{Namespace: testCmdNamespace, Storage: testCmdStorage}
}

// testMinimalRestore returns a restore request with only the required fields
// and the given policy set.
func testMinimalRestore(policy infomodels.RestorePolicy) *infomodels.RequestRestore {
	return &infomodels.RequestRestore{
		RequestCommon: testCmdMinimalCommon(),
		BackupIDs:     testCmdBackupID,
		Policy:        policy,
	}
}

// testSetFuzzyParams sets every parameter of r that only the fuzzy policy accepts.
func testSetFuzzyParams(r *infomodels.RequestRestore) {
	r.MaxInflight = 400
	r.RetryBaseIntervalMs = new(250)
	r.RetryMultiplier = 1.5
	r.RetryMaxAttempts = new(3)
	r.IgnoreRecordError = new(false)
	r.ApplyMetadataLast = new(true)
}

// testSetColdParams sets every parameter of r that only the strict policy accepts.
func testSetColdParams(r *infomodels.RequestRestore) {
	r.HydrateReplica = new(false)
}

// testVersionBeforeIntegratedBackup returns a server version that has neither
// the integrated backup nor the recent info command syntax.
func testVersionBeforeIntegratedBackup() infomodels.AerospikeVersion {
	return infomodels.AerospikeVersion{Major: 8, Minor: 0, Patch: 0}
}

// integratedBackupCall runs one integrated backup command builder. want is the
// command it builds on a server that supports the integrated backup.
type integratedBackupCall struct {
	name string
	call func(c infoCommands) (string, error)
	want string
}

// integratedBackupCalls returns every integrated backup command builder with
// all required parameters set.
func integratedBackupCalls() []integratedBackupCall {
	return []integratedBackupCall{
		{
			name: cmdNameBackup,
			call: func(c infoCommands) (string, error) {
				return c.serverBackup(&infomodels.RequestBackup{
					RequestCommon: infomodels.RequestCommon{
						Namespace: testCmdNamespace,
						Storage:   testCmdStorage,
					},
				}, testCmdJobID)
			},
			want: testCmdBackupBase,
		},
		{
			name: cmdNameRestore,
			call: func(c infoCommands) (string, error) {
				return c.serverRestore(&infomodels.RequestRestore{
					RequestCommon: infomodels.RequestCommon{
						Namespace: testCmdNamespace,
						Storage:   testCmdStorage,
					},
					BackupIDs: testCmdBackupID,
				}, testCmdJobID)
			},
			want: testCmdRestoreBase,
		},
		{
			name: cmdNameBackupStatus,
			call: func(c infoCommands) (string, error) { return c.backupStatus(testCmdJobID) },
			want: "backup-status:job-id=260922T103653-5xsr",
		},
		{
			name: cmdNameRestoreStatus,
			call: func(c infoCommands) (string, error) { return c.restoreStatus(testCmdJobID) },
			want: "restore-status:job-id=260922T103653-5xsr",
		},
		{
			name: cmdNameBackupAbort,
			call: func(c infoCommands) (string, error) { return c.backupAbort(testCmdJobID) },
			want: "backup-abort:job-id=260922T103653-5xsr",
		},
		{
			name: cmdNameRestoreAbort,
			call: func(c infoCommands) (string, error) {
				return c.restoreAbort(testCmdNamespace, testCmdJobID)
			},
			want: "restore-abort:namespace=source-ns;job-id=260922T103653-5xsr",
		},
		{
			name: cmdNameRestoreOpen,
			call: func(c infoCommands) (string, error) {
				return c.restoreOpen(testCmdNamespace)
			},
			want: "restore-open:namespace=source-ns",
		},
		{
			name: cmdNameRestoreClearStorage,
			call: func(c infoCommands) (string, error) {
				return c.restoreClearStorage(testCmdNamespace)
			},
			want: "restore-clear-storage:namespace=source-ns",
		},
	}
}

func TestInfoCommands_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "build", got: cmdBuild, want: "build"},
		{name: "status", got: cmdStatus, want: "status"},
		{name: "namespaces", got: cmdNamespaces, want: "namespaces"},
		{name: "service clear", got: cmdServiceClearStd, want: "service-clear-std"},
		{name: "service tls", got: cmdServiceTLSStd, want: "service-tls-std"},
		{name: "udf list", got: cmdUdfList, want: "udf-list"},
		{name: "statistics", got: cmdStatistics, want: "statistics"},
		{name: "query show", got: cmdShowJobsQueries, want: "query-show"},
		{name: "racks", got: cmdRacks, want: "racks:"},
		{name: "replicas", got: cmdReplicas, want: "replicas:max=1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestInfoCommands_AllVersions(t *testing.T) {
	t.Parallel()

	const (
		testUDFName     = "test.lua"
		testClusterSize = 3
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionRecentInfoCommands)

	tests := []struct {
		name    string
		call    func() (string, error)
		want    string
		wantErr error
	}{
		{
			name: "sets of namespace",
			call: func() (string, error) { return cmds.setsOfNamespace(testCmdNamespace) },
			want: "sets/source-ns",
		},
		{
			name:    "sets of namespace without namespace",
			call:    func() (string, error) { return cmds.setsOfNamespace("") },
			wantErr: errMissingCmdParam,
		},
		{
			name:    "sets of namespace with separator",
			call:    func() (string, error) { return cmds.setsOfNamespace(testUnsafeValue) },
			wantErr: errInvalidCmdParam,
		},
		{
			name: "namespace info",
			call: func() (string, error) { return cmds.namespaceInfo(testCmdNamespace) },
			want: "namespace/source-ns",
		},
		{
			name:    "namespace info without namespace",
			call:    func() (string, error) { return cmds.namespaceInfo("") },
			wantErr: errMissingCmdParam,
		},
		{
			name:    "namespace info with separator",
			call:    func() (string, error) { return cmds.namespaceInfo(testUnsafeValue) },
			wantErr: errInvalidCmdParam,
		},
		{
			name: "udf get",
			call: func() (string, error) { return cmds.udfGet(testUDFName) },
			want: "udf-get:filename=test.lua",
		},
		{
			name:    "udf get without filename",
			call:    func() (string, error) { return cmds.udfGet("") },
			wantErr: errMissingCmdParam,
		},
		{
			name:    "udf get with separator",
			call:    func() (string, error) { return cmds.udfGet(testUnsafeValue) },
			wantErr: errInvalidCmdParam,
		},
		{
			name: "cluster stable",
			call: func() (string, error) { return cmds.clusterStable(testClusterSize, testCmdNamespace) },
			want: "cluster-stable:size=3;ignore-migrations=false;namespace=source-ns",
		},
		{
			name:    "cluster stable without namespace",
			call:    func() (string, error) { return cmds.clusterStable(testClusterSize, "") },
			wantErr: errMissingCmdParam,
		},
		{
			name:    "cluster stable with separator",
			call:    func() (string, error) { return cmds.clusterStable(testClusterSize, testUnsafeValue) },
			wantErr: errInvalidCmdParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				assert.NotContains(t, err.Error(), testUnsafeValue)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_ValidateClusterStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		giveNamespace string
		wantErr       error
	}{
		{
			name:          "valid",
			giveNamespace: testCmdNamespace,
		},
		{
			name:    "namespace missing",
			wantErr: errMissingCmdParam,
		},
		{
			name:          "separator in namespace",
			giveNamespace: testUnsafeValue,
			wantErr:       errInvalidCmdParam,
		},
		{
			name:          "newline in namespace",
			giveNamespace: testNewlineValue,
			wantErr:       errInvalidCmdParam,
		},
	}

	cmds := newInfoCommands(infomodels.AerospikeVersionRecentInfoCommands)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cmds.validateClusterStable(tt.giveNamespace)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInfoCommands_SindexList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		giveVersion   infomodels.AerospikeVersion
		giveNamespace string
		giveWithCtx   bool
		want          string
		wantErr       error
	}{
		{
			name:          "before 8.1 without ctx",
			giveVersion:   testVersionBeforeIntegratedBackup(),
			giveNamespace: testCmdNamespace,
			want:          "sindex-list:ns=source-ns",
		},
		{
			name:          "before 8.1 with ctx",
			giveVersion:   testVersionBeforeIntegratedBackup(),
			giveNamespace: testCmdNamespace,
			giveWithCtx:   true,
			want:          "sindex-list:ns=source-ns;b64=true",
		},
		{
			name:          "since 8.1 without ctx",
			giveVersion:   infomodels.AerospikeVersionRecentInfoCommands,
			giveNamespace: testCmdNamespace,
			want:          "sindex-list:namespace=source-ns",
		},
		{
			name:          "since 8.1 with ctx",
			giveVersion:   infomodels.AerospikeVersionRecentInfoCommands,
			giveNamespace: testCmdNamespace,
			giveWithCtx:   true,
			want:          "sindex-list:namespace=source-ns;b64=true",
		},
		{
			name:        "before 8.1 without namespace",
			giveVersion: testVersionBeforeIntegratedBackup(),
			wantErr:     errMissingCmdParam,
		},
		{
			name:        "since 8.1 without namespace",
			giveVersion: infomodels.AerospikeVersionRecentInfoCommands,
			wantErr:     errMissingCmdParam,
		},
		{
			name:          "namespace with separator",
			giveVersion:   infomodels.AerospikeVersionRecentInfoCommands,
			giveNamespace: testUnsafeValue,
			wantErr:       errInvalidCmdParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newInfoCommands(tt.giveVersion).sindexList(tt.giveNamespace, tt.giveWithCtx)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				assert.NotContains(t, err.Error(), testUnsafeValue)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_IntegratedBackup(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	for _, tt := range integratedBackupCalls() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call(cmds)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_IntegratedBackupNotSupported(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(testVersionBeforeIntegratedBackup())

	for _, tt := range integratedBackupCalls() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call(cmds)

			require.ErrorIs(t, err, errCommandNotSupported)
			require.ErrorIs(t, err, errclass.ErrUnsupported)
			require.ErrorContains(t, err, tt.name)
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_IntegratedBackupMissingParams(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name        string
		call        func() (string, error)
		wantMissing string
	}{
		{
			name: cmdNameBackup,
			call: func() (string, error) {
				return cmds.serverBackup(&infomodels.RequestBackup{}, "")
			},
			wantMissing: "namespace, job-id, object-storage-type",
		},
		{
			name: cmdNameRestore,
			call: func() (string, error) {
				return cmds.serverRestore(&infomodels.RequestRestore{}, "")
			},
			wantMissing: "namespace, job-id, backup-ids, object-storage-type",
		},
		{
			name:        cmdNameBackupStatus,
			call:        func() (string, error) { return cmds.backupStatus("") },
			wantMissing: paramJobID,
		},
		{
			name:        cmdNameRestoreStatus,
			call:        func() (string, error) { return cmds.restoreStatus("") },
			wantMissing: paramJobID,
		},
		{
			name:        cmdNameBackupAbort,
			call:        func() (string, error) { return cmds.backupAbort("") },
			wantMissing: paramJobID,
		},
		{
			name:        cmdNameRestoreAbort,
			call:        func() (string, error) { return cmds.restoreAbort("", "") },
			wantMissing: testCmdMissingNsAndJobID,
		},
		{
			name:        cmdNameRestoreOpen,
			call:        func() (string, error) { return cmds.restoreOpen("") },
			wantMissing: paramNamespace,
		},
		{
			name:        cmdNameRestoreClearStorage,
			call:        func() (string, error) { return cmds.restoreClearStorage("") },
			wantMissing: paramNamespace,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()

			require.ErrorIs(t, err, errMissingCmdParam)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
			require.ErrorContains(t, err, tt.name+": "+tt.wantMissing)
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_NilRequest(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name string
		call func() (string, error)
	}{
		{
			name: cmdNameBackup,
			call: func() (string, error) { return cmds.serverBackup(nil, testCmdJobID) },
		},
		{
			name: cmdNameRestore,
			call: func() (string, error) { return cmds.serverRestore(nil, testCmdJobID) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()

			require.ErrorIs(t, err, errNilRequest)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_ValidateRestoreStatusNamespace(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		wantErrIs error
		name      string
		give      string
	}{
		{
			name: "valid",
			give: testCmdNamespace,
		},
		{
			name:      testNameEmpty,
			wantErrIs: errMissingCmdParam,
		},
		{
			name:      "with separator",
			give:      testUnsafeValue,
			wantErrIs: errInvalidCmdParam,
		},
		{
			name:      "with pipe",
			give:      testPipeValue,
			wantErrIs: errInvalidCmdParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cmds.validateRestoreStatusNamespace(tt.give)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorContains(t, err, cmdNameRestoreStatus+": "+paramNamespace)

				if tt.give != "" {
					assert.NotContains(t, err.Error(), tt.give)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInfoCommands_InvalidParams(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	backup := func(setList string) (string, error) {
		return cmds.serverBackup(&infomodels.RequestBackup{
			RequestCommon: infomodels.RequestCommon{
				Namespace: testCmdNamespace,
				Storage:   testCmdStorage,
				SetList:   setList,
			},
		}, testCmdJobID)
	}

	restore := func(path, backupIDs string) (string, error) {
		return cmds.serverRestore(&infomodels.RequestRestore{
			RequestCommon: infomodels.RequestCommon{
				Namespace: testCmdNamespace,
				Storage:   testCmdStorage,
				Path:      path,
			},
			BackupIDs: backupIDs,
		}, testCmdJobID)
	}

	const (
		testInvalidBackupIDsCommaOnly    = ","
		testInvalidBackupIDsEmptyEntry   = "260901T000000-abcd,,260901T000001-efgh"
		testInvalidBackupIDsLeadingComma = ",260901T000000-abcd"
	)

	tests := []struct {
		name      string
		giveValue string
		call      func(value string) (string, error)
	}{
		{
			name:      "backup value with separator",
			giveValue: testUnsafeValue,
			call:      backup,
		},
		{
			name:      "backup value with pipe",
			giveValue: testPipeValue,
			call:      backup,
		},
		{
			name:      "restore value with separator",
			giveValue: testUnsafeValue,
			call:      func(value string) (string, error) { return restore(testCmdPath, value) },
		},
		{
			name:      "restore required value with pipe",
			giveValue: testPipeValue,
			call:      func(value string) (string, error) { return restore(testCmdPath, value) },
		},
		{
			name:      "restore storage value with pipe",
			giveValue: testPipeValue,
			call:      func(value string) (string, error) { return restore(value, testCmdBackupID) },
		},
		{
			name:      "backup status job id with pipe",
			giveValue: testPipeValue,
			call:      cmds.backupStatus,
		},
		{
			name:      "backup abort job id with pipe",
			giveValue: testPipeValue,
			call:      cmds.backupAbort,
		},
		{
			name:      "restore status job id with pipe",
			giveValue: testPipeValue,
			call:      cmds.restoreStatus,
		},
		{
			name:      "restore abort job id with pipe",
			giveValue: testPipeValue,
			call:      func(value string) (string, error) { return cmds.restoreAbort(testCmdNamespace, value) },
		},
		{
			name:      "restore open namespace with pipe",
			giveValue: testPipeValue,
			call:      cmds.restoreOpen,
		},
		{
			name:      "restore clear storage namespace with pipe",
			giveValue: testPipeValue,
			call:      cmds.restoreClearStorage,
		},
		{
			name:      "restore backup ids comma only",
			giveValue: testInvalidBackupIDsCommaOnly,
			call:      func(value string) (string, error) { return restore(testCmdPath, value) },
		},
		{
			name:      "restore backup ids empty entry",
			giveValue: testInvalidBackupIDsEmptyEntry,
			call:      func(value string) (string, error) { return restore(testCmdPath, value) },
		},
		{
			name:      "restore backup ids leading comma",
			giveValue: testInvalidBackupIDsLeadingComma,
			call:      func(value string) (string, error) { return restore(testCmdPath, value) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call(tt.giveValue)

			require.ErrorIs(t, err, errInvalidCmdParam)
			assert.NotContains(t, err.Error(), tt.giveValue)
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_ServerBackup(t *testing.T) {
	t.Parallel()

	const (
		testModifiedBefore     = "1700000100"
		testModifiedAfter      = "1700000000"
		testModifiedAfterParam = ";modified-after=1700000000"
		testBinList            = "bin1,bin2"
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	allFields := testStorage()
	allFields.SetList = testCmdSetList
	allFields.FilterExp = testCmdFilterExp
	allFields.NoIndexes = new(true)
	allFields.NoUDFs = new(false)

	tests := []struct {
		name string
		give *infomodels.RequestBackup
		want string
	}{
		{
			name: "all fields set",
			give: &infomodels.RequestBackup{
				RequestCommon:                allFields,
				BinList:                      testBinList,
				ModifiedBefore:               testModifiedBefore,
				ModifiedAfter:                testModifiedAfter,
				BackupRPS:                    1000,
				BackupWorkerThreads:          8,
				BackupUploadThreads:          16,
				BackupS3ThroughputTargetGbps: 25,
				Policy:                       infomodels.BackupPolicyBestEffort,
				NoConfig:                     new(true),
			},
			want: testCmdBackupBase + testCmdStorageParams + testCmdSetListParam + ";bin-list=bin1,bin2" +
				testModifiedAfterParam + ";modified-before=1700000100" + testCmdFilterExpParam +
				";no-indexes=true;no-udfs=false;no-config=true;backup-rps=1000;backup-worker-threads=8;" +
				"backup-upload-threads=16;backup-s3-throughput-target-gbps=25;backup-policy=best-effort",
		},
		{
			name: "strict policy",
			give: &infomodels.RequestBackup{
				RequestCommon: testCmdMinimalCommon(),
				Policy:        infomodels.BackupPolicyStrict,
			},
			want: testCmdBackupBase + ";backup-policy=strict",
		},
		{
			name: "fuzzy policy",
			give: &infomodels.RequestBackup{
				RequestCommon: testCmdMinimalCommon(),
				Policy:        infomodels.BackupPolicyFuzzy,
			},
			want: testCmdBackupBase + ";backup-policy=fuzzy",
		},
		{
			name: testNameUnsetOmitted,
			give: &infomodels.RequestBackup{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
					Bucket:    testCmdBucket,
				},
				ModifiedAfter: testModifiedAfter,
			},
			want: testCmdBackupBase + ";s3-bucket=backup-bucket" + testModifiedAfterParam,
		},
		{
			name: "false flag is sent when set",
			give: &infomodels.RequestBackup{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
					NoUDFs:    new(false),
				},
			},
			want: testCmdBackupBase + ";no-udfs=false",
		},
		{
			name: "false no config is sent when set",
			give: &infomodels.RequestBackup{
				RequestCommon: testCmdMinimalCommon(),
				NoConfig:      new(false),
			},
			want: testCmdBackupBase + ";no-config=false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverBackup(tt.give, testCmdJobID)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_ServerRestore(t *testing.T) {
	t.Parallel()

	const (
		testIncrementalPath = "/backups/incr"
		testRestoreFlags    = ";no-indexes=false;no-udfs=true"
		// testHead is the parameters of the requests below that precede the set
		// list, in the order the command adds them.
		testHead = "restore:namespace=" + testCmdNamespace + ";job-id=" + testCmdJobID +
			";backup-ids=" + testCmdBackupIDsChain + ";object-storage-type=" + testCmdStorage +
			testCmdStorageParams + ";incremental-path=" + testIncrementalPath + testRestoreFlags
		// testRestoreLimits are the parameters of both modes that follow the filter.
		testRestoreLimits = ";restore-download-threads=8;restore-worker-threads=16;restore-rps=5000;" +
			"extra-ttl=3600"
		// testColdParams are the cold only parameters set by testSetColdParams.
		testColdParams = ";hydrate-replica=false"
		// testFuzzyParams are the fuzzy only parameters set by testSetFuzzyParams.
		testFuzzyParams = ";max-inflight=400;retry-base-interval=250;" +
			"retry-multiplier=1.5;retry-max-attempts=3;ignore-record-error=false;apply-metadata-last=true"
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	// newRequest returns a request with every parameter of both policies set.
	newRequest := func(policy infomodels.RestorePolicy) *infomodels.RequestRestore {
		common := testStorage()
		common.SetList = testCmdSetList
		common.FilterExp = testCmdFilterExp
		common.NoIndexes = new(false)
		common.NoUDFs = new(true)

		return &infomodels.RequestRestore{
			RequestCommon:          common,
			BackupIDs:              testCmdBackupIDsChain,
			IncrementalPath:        testIncrementalPath,
			RestoreDownloadThreads: 8,
			RestoreWorkerThreads:   16,
			RestoreRPS:             5000,
			ExtraTTL:               3600,
			Policy:                 policy,
		}
	}

	strictRequest := newRequest(infomodels.RestorePolicyStrict)
	testSetColdParams(strictRequest)

	defaultRequest := newRequest("")
	testSetColdParams(defaultRequest)

	hydrateAllRequest := testMinimalRestore("")
	hydrateAllRequest.HydrateReplica = new(true)

	fuzzyRequest := newRequest(infomodels.RestorePolicyFuzzy)
	testSetFuzzyParams(fuzzyRequest)

	minimalFuzzyRequest := testMinimalRestore(infomodels.RestorePolicyFuzzy)

	zeroIntervalRequest := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	zeroIntervalRequest.RetryBaseIntervalMs = new(0)

	noRetryRequest := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	noRetryRequest.RetryMaxAttempts = new(0)

	tests := []struct {
		name string
		give *infomodels.RequestRestore
		want string
	}{
		{
			name: "strict policy sends every parameter it accepts",
			give: strictRequest,
			want: testHead + testCmdSetListParam + testCmdFilterExpParam + testRestoreLimits +
				testCmdRestorePolicyStrict + testColdParams,
		},
		{
			name: "empty policy is not sent and accepts every parameter of strict policy",
			give: defaultRequest,
			want: testHead + testCmdSetListParam + testCmdFilterExpParam + testRestoreLimits + testColdParams,
		},
		{
			name: "hydrate replica true is sent",
			give: hydrateAllRequest,
			want: testCmdRestoreBase + ";hydrate-replica=true",
		},
		{
			name: "fuzzy policy sends every parameter it accepts",
			give: fuzzyRequest,
			want: testHead + testCmdSetListParam + testCmdFilterExpParam + testRestoreLimits +
				testCmdRestorePolicyFuzzy +
				testFuzzyParams,
		},
		{
			name: testNameUnsetOmitted,
			give: minimalFuzzyRequest,
			want: testCmdRestoreBase + testCmdRestorePolicyFuzzy,
		},
		{
			name: "zero retry base interval is sent",
			give: zeroIntervalRequest,
			want: testCmdRestoreBase + testCmdRestorePolicyFuzzy + ";retry-base-interval=0",
		},
		{
			name: "zero retry max attempts is sent",
			give: noRetryRequest,
			want: testCmdRestoreBase + testCmdRestorePolicyFuzzy + ";retry-max-attempts=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverRestore(tt.give, testCmdJobID)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_ServerRestorePolicyMismatch(t *testing.T) {
	t.Parallel()

	const (
		// testStrictPrefix starts the report of the fuzzy only parameters set
		// for the strict policy.
		testStrictPrefix = "restore with restore-policy=strict: "
		// testFuzzyPrefix starts the report of the cold only parameters set
		// for the fuzzy policy.
		testFuzzyPrefix = "restore with restore-policy=fuzzy: "
		// testDefaultPrefix starts the same report for an empty policy.
		testDefaultPrefix = "restore with default restore-policy (strict): "
		// testIncrementalOneID reports an incremental path for one backup id.
		testIncrementalOneID = "restore with one backup id: " + paramIncrementalPath
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	fuzzyWithIncrementalPath := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	fuzzyWithIncrementalPath.IncrementalPath = testCmdPath

	strictWithIncrementalPath := testMinimalRestore(infomodels.RestorePolicyStrict)
	strictWithIncrementalPath.IncrementalPath = testCmdPath

	strictWithMaxInflightAndIncrementalPath := testMinimalRestore(infomodels.RestorePolicyStrict)
	strictWithMaxInflightAndIncrementalPath.MaxInflight = 1
	strictWithMaxInflightAndIncrementalPath.IncrementalPath = testCmdPath

	fuzzyWithColdParams := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	testSetColdParams(fuzzyWithColdParams)

	fuzzyOnFS := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	fuzzyOnFS.Storage = testCmdStorageFS

	fuzzyOnFA := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	fuzzyOnFA.Storage = testCmdStorageFA

	fuzzyApplyNothing := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	fuzzyApplyNothing.NoIndexes = new(true)
	fuzzyApplyNothing.NoUDFs = new(true)
	fuzzyApplyNothing.ApplyMetadataLast = new(false)

	strictWithFuzzyParams := testMinimalRestore(infomodels.RestorePolicyStrict)
	testSetFuzzyParams(strictWithFuzzyParams)

	defaultWithMaxInflight := testMinimalRestore("")
	defaultWithMaxInflight.MaxInflight = 1

	strictWithZeroInterval := testMinimalRestore(infomodels.RestorePolicyStrict)
	strictWithZeroInterval.RetryBaseIntervalMs = new(0)

	strictWithMaxInflightNoNamespace := testMinimalRestore(infomodels.RestorePolicyStrict)
	strictWithMaxInflightNoNamespace.MaxInflight = 1
	strictWithMaxInflightNoNamespace.Namespace = ""

	tests := []struct {
		name        string
		give        *infomodels.RequestRestore
		wantErrText string
		wantMissing bool
	}{
		{
			name:        "incremental path for one backup id with fuzzy policy",
			give:        fuzzyWithIncrementalPath,
			wantErrText: testIncrementalOneID,
		},
		{
			name:        "incremental path for one backup id with strict policy",
			give:        strictWithIncrementalPath,
			wantErrText: testIncrementalOneID,
		},
		{
			name: "policy mismatch is reported with the incremental path",
			give: strictWithMaxInflightAndIncrementalPath,
			wantErrText: testStrictPrefix + paramMaxInflight + "; " + errParamNotApplicable.Error() + ": " +
				testIncrementalOneID,
		},
		{
			name: "every fuzzy only parameter with strict policy",
			give: strictWithFuzzyParams,
			wantErrText: testStrictPrefix + "max-inflight, retry-base-interval, " +
				"retry-multiplier, retry-max-attempts, ignore-record-error, apply-metadata-last",
		},
		{
			name:        "every cold only parameter with fuzzy policy",
			give:        fuzzyWithColdParams,
			wantErrText: testFuzzyPrefix + paramHydrateReplica,
		},
		{
			name:        "fuzzy policy on file system storage",
			give:        fuzzyOnFS,
			wantErrText: testRestoreOnStorage + testCmdStorageFS + ": " + paramRestorePolicy,
		},
		{
			name:        "fuzzy policy on file system storage fa",
			give:        fuzzyOnFA,
			wantErrText: testRestoreOnStorage + testCmdStorageFA + ": " + paramRestorePolicy,
		},
		{
			name:        "apply metadata last with nothing to apply",
			give:        fuzzyApplyNothing,
			wantErrText: "restore with no-indexes=true and no-udfs=true: " + paramApplyMetadataLast,
		},
		{
			name:        "fuzzy only parameter with empty policy",
			give:        defaultWithMaxInflight,
			wantErrText: testDefaultPrefix + paramMaxInflight,
		},
		{
			name:        "zero retry base interval with strict policy",
			give:        strictWithZeroInterval,
			wantErrText: testStrictPrefix + paramRetryBaseInterval,
		},
		{
			name:        "missing parameter is reported with the policy mismatch",
			give:        strictWithMaxInflightNoNamespace,
			wantErrText: testStrictPrefix + paramMaxInflight,
			wantMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverRestore(tt.give, testCmdJobID)

			require.ErrorIs(t, err, errParamNotApplicable)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErrText)
			assert.Equal(t, tt.wantMissing, errors.Is(err, errMissingCmdParam))
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_ServerRestoreFuzzyInvalidBackupIDs(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name string
		give string
	}{
		{name: "trailing comma", give: testCmdBackupID + ","},
		{name: "empty entry", give: testCmdBackupID + ",," + testCmdBackupID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := testMinimalRestore(infomodels.RestorePolicyFuzzy)
			r.BackupIDs = tt.give

			got, err := cmds.serverRestore(r, testCmdJobID)

			require.ErrorIs(t, err, errInvalidCmdParam)
			require.NotErrorIs(t, err, errParamNotApplicable)
			assert.Empty(t, got)
		})
	}
}

func TestBackupIDCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give string
		want int
	}{
		{name: testNameEmpty, give: "", want: 0},
		{name: "one id", give: testCmdBackupID, want: 1},
		{name: "chain", give: testCmdBackupIDsChain, want: 2},
		{name: "invalid list", give: testCmdBackupID + ",", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, backupIDCount(tt.give))
		})
	}
}

func TestJoinErrs(t *testing.T) {
	t.Parallel()

	errFirst := errors.New("first")
	errSecond := errors.New("second")

	tests := []struct {
		name     string
		give     []error
		wantText string
		wantIs   []error
	}{
		{name: "none", give: nil},
		{name: "only nil", give: []error{nil, nil}},
		{
			name:     "one",
			give:     []error{nil, errFirst},
			wantText: errFirst.Error(),
			wantIs:   []error{errFirst},
		},
		{
			name:     "several on one line",
			give:     []error{errFirst, nil, errSecond},
			wantText: "first; second",
			wantIs:   []error{errFirst, errSecond},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := joinErrs(tt.give...)
			if tt.wantText == "" {
				require.NoError(t, err)

				return
			}

			require.EqualError(t, err, tt.wantText)

			for _, want := range tt.wantIs {
				require.ErrorIs(t, err, want)
			}
		})
	}
}

func TestInfoCommands_UnknownPolicy(t *testing.T) {
	t.Parallel()

	const (
		// testUnknownPolicy is a policy neither the backup nor the restore knows.
		testUnknownPolicy = "best-fuzzy"
		// testBackupPolicyErr and testRestorePolicyErr are how the errors
		// report testUnknownPolicy.
		testBackupPolicyErr  = cmdNameBackup + ": " + paramBackupPolicy + `="` + testUnknownPolicy + `"`
		testRestorePolicyErr = cmdNameRestore + ": " + paramRestorePolicy + `="` + testUnknownPolicy + `"`
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	backup := func(jobID string) (string, error) {
		return cmds.serverBackup(&infomodels.RequestBackup{
			RequestCommon: testCmdMinimalCommon(),
			Policy:        testUnknownPolicy,
		}, jobID)
	}

	restoreWithFuzzyParams := testMinimalRestore(testUnknownPolicy)
	testSetFuzzyParams(restoreWithFuzzyParams)

	restoreWithFilter := testMinimalRestore(testUnknownPolicy)
	restoreWithFilter.FilterExp = testCmdFilterExp

	restoreNoNamespace := testMinimalRestore(testUnknownPolicy)
	restoreNoNamespace.Namespace = ""

	tests := []struct {
		name        string
		call        func() (string, error)
		wantErrText string
		wantMissing bool
	}{
		{
			name:        "backup",
			call:        func() (string, error) { return backup(testCmdJobID) },
			wantErrText: testBackupPolicyErr,
		},
		{
			name:        "backup with missing parameter",
			call:        func() (string, error) { return backup("") },
			wantErrText: testBackupPolicyErr,
			wantMissing: true,
		},
		{
			name: "restore",
			call: func() (string, error) {
				return cmds.serverRestore(testMinimalRestore(testUnknownPolicy), testCmdJobID)
			},
			wantErrText: testRestorePolicyErr,
		},
		{
			name:        "restore with fuzzy only parameters",
			call:        func() (string, error) { return cmds.serverRestore(restoreWithFuzzyParams, testCmdJobID) },
			wantErrText: testRestorePolicyErr,
		},
		{
			name:        "restore with filter expression",
			call:        func() (string, error) { return cmds.serverRestore(restoreWithFilter, testCmdJobID) },
			wantErrText: testRestorePolicyErr,
		},
		{
			name:        "restore with missing parameter",
			call:        func() (string, error) { return cmds.serverRestore(restoreNoNamespace, testCmdJobID) },
			wantErrText: testRestorePolicyErr,
			wantMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()

			require.ErrorIs(t, err, errUnknownPolicy)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErrText)
			require.NotErrorIs(t, err, errParamNotApplicable)
			assert.Equal(t, tt.wantMissing, errors.Is(err, errMissingCmdParam))
			assert.Empty(t, got)
		})
	}
}

func TestCheckPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		give        infomodels.RestorePolicy
		wantErrText string
	}{
		{name: testNameEmpty, give: ""},
		{name: testNameStrict, give: infomodels.RestorePolicyStrict},
		{name: testNameFuzzy, give: infomodels.RestorePolicyFuzzy},
		{name: testNameUnknown, give: testPolicyCold, wantErrText: `restore: restore-policy="cold"`},
		{name: "case sensitive", give: "Strict", wantErrText: `restore: restore-policy="Strict"`},
		{name: "newline is quoted", give: "a\nb", wantErrText: `restore: restore-policy="a\nb"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := checkPolicy(cmdNameRestore, paramRestorePolicy, tt.give, knownRestorePolicy)
			if tt.wantErrText != "" {
				require.ErrorIs(t, err, errUnknownPolicy)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				require.ErrorContains(t, err, tt.wantErrText)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestKnownBackupPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give infomodels.BackupPolicy
		want bool
	}{
		{name: testNameStrict, give: infomodels.BackupPolicyStrict, want: true},
		{name: "best effort", give: infomodels.BackupPolicyBestEffort, want: true},
		{name: testNameFuzzy, give: infomodels.BackupPolicyFuzzy, want: true},
		{name: testNameEmpty, give: "", want: false},
		{name: testNameUnknown, give: testPolicyCold, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, knownBackupPolicy(tt.give))
		})
	}
}

func TestKnownRestorePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give infomodels.RestorePolicy
		want bool
	}{
		{name: testNameStrict, give: infomodels.RestorePolicyStrict, want: true},
		{name: testNameFuzzy, give: infomodels.RestorePolicyFuzzy, want: true},
		{name: testNameEmpty, give: "", want: false},
		{name: "backup only policy", give: "best-effort", want: false},
		{name: testNameUnknown, give: testPolicyCold, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, knownRestorePolicy(tt.give))
		})
	}
}

func TestInfoCommands_ServerBackupStorageMismatch(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	backup := func(storage infomodels.StorageType, policy infomodels.BackupPolicy) *infomodels.RequestBackup {
		return &infomodels.RequestBackup{
			RequestCommon: infomodels.RequestCommon{Namespace: testCmdNamespace, Storage: storage},
			Policy:        policy,
		}
	}

	// accepted returns the command of an accepted request built by backup.
	accepted := func(storage infomodels.StorageType, policy string) string {
		return "backup:namespace=" + testCmdNamespace + ";job-id=" + testCmdJobID +
			";object-storage-type=" + string(storage) + policy
	}

	tests := []struct {
		name        string
		give        *infomodels.RequestBackup
		want        string
		wantErrText string
	}{
		{
			name:        "best effort on fs",
			give:        backup(testCmdStorageFS, infomodels.BackupPolicyBestEffort),
			wantErrText: testBackupOnStorage + testCmdStorageFS + ": " + paramBackupPolicy,
		},
		{
			name:        "fuzzy on fa",
			give:        backup(testCmdStorageFA, infomodels.BackupPolicyFuzzy),
			wantErrText: testBackupOnStorage + testCmdStorageFA + ": " + paramBackupPolicy,
		},
		{
			name: "strict on fs",
			give: backup(testCmdStorageFS, infomodels.BackupPolicyStrict),
			want: accepted(testCmdStorageFS, ";backup-policy=strict"),
		},
		{
			name: "default policy on fs",
			give: backup(testCmdStorageFS, ""),
			want: accepted(testCmdStorageFS, ""),
		},
		{
			name: "fuzzy on object store",
			give: backup(testCmdStorage, infomodels.BackupPolicyFuzzy),
			want: accepted(testCmdStorage, ";backup-policy=fuzzy"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverBackup(tt.give, testCmdJobID)
			if tt.wantErrText != "" {
				require.ErrorIs(t, err, errParamNotApplicable)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				require.ErrorContains(t, err, tt.wantErrText)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_ServerRestoreAccepted(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	strictOnFS := testMinimalRestore(infomodels.RestorePolicyStrict)
	strictOnFS.Storage = testCmdStorageFS

	fuzzyApplyIndexes := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	fuzzyApplyIndexes.NoIndexes = new(false)
	fuzzyApplyIndexes.NoUDFs = new(true)
	fuzzyApplyIndexes.ApplyMetadataLast = new(true)

	fuzzyNothingToApplyUnset := testMinimalRestore(infomodels.RestorePolicyFuzzy)
	fuzzyNothingToApplyUnset.NoIndexes = new(true)
	fuzzyNothingToApplyUnset.NoUDFs = new(true)

	const (
		testStrictOnFS = "restore:namespace=" + testCmdNamespace + ";job-id=" + testCmdJobID +
			";backup-ids=" + testCmdBackupID + ";object-storage-type=" + testCmdStorageFS +
			testCmdRestorePolicyStrict
		testNoUDFs = ";no-udfs=true"
	)

	tests := []struct {
		name string
		give *infomodels.RequestRestore
		want string
	}{
		{
			name: "strict policy on file system storage",
			give: strictOnFS,
			want: testStrictOnFS,
		},
		{
			name: "apply metadata last with indexes to apply",
			give: fuzzyApplyIndexes,
			want: testCmdRestoreBase + ";no-indexes=false" + testNoUDFs + testCmdRestorePolicyFuzzy +
				";apply-metadata-last=true",
		},
		{
			name: "nothing to apply without apply metadata last",
			give: fuzzyNothingToApplyUnset,
			want: testCmdRestoreBase + ";no-indexes=true" + testNoUDFs + testCmdRestorePolicyFuzzy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverRestore(tt.give, testCmdJobID)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestInfoCommands_InvalidJobID checks that every command taking a job id
// rejects an id the server refuses (§1.3) before it is sent, and that the
// restore command does the same for its backup ids. The ids have no "|", so
// only the id rules can reject them.
func TestInfoCommands_InvalidJobID(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	ids := []struct {
		name string
		give string
	}{
		{name: "slash", give: "a/b"},
		{name: "double dot", give: "a..b"},
		{name: "space", give: "a b"},
		{name: "too long", give: strings.Repeat("x", idMaxLen+1)},
	}

	calls := []struct {
		build func(id string) (string, error)
		name  string
	}{
		{
			name: cmdNameBackup,
			build: func(id string) (string, error) {
				return cmds.serverBackup(&infomodels.RequestBackup{RequestCommon: testCmdMinimalCommon()}, id)
			},
		},
		{
			name: cmdNameRestore,
			build: func(id string) (string, error) {
				return cmds.serverRestore(testMinimalRestore(""), id)
			},
		},
		{name: cmdNameBackupStatus, build: cmds.backupStatus},
		{name: cmdNameRestoreStatus, build: cmds.restoreStatus},
		{name: cmdNameBackupAbort, build: cmds.backupAbort},
		{
			name: cmdNameRestoreAbort,
			build: func(id string) (string, error) {
				return cmds.restoreAbort(testCmdNamespace, id)
			},
		},
	}

	for _, c := range calls {
		for _, id := range ids {
			t.Run(c.name+" "+id.name, func(t *testing.T) {
				t.Parallel()

				got, err := c.build(id.give)

				require.ErrorIs(t, err, errInvalidCmdParam)
				require.ErrorContains(t, err, paramJobID)
				assert.Empty(t, got)
			})
		}
	}
}

// TestInfoCommands_InvalidBackupIDs checks the backup id list rules of the
// restore command (§6.1): 1 to 32 distinct valid ids.
func TestInfoCommands_InvalidBackupIDs(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	listOf := func(n int) string {
		ids := make([]string, 0, n)
		for i := range n {
			ids = append(ids, "b"+strconv.Itoa(i))
		}

		return strings.Join(ids, ",")
	}

	tests := []struct {
		name    string
		give    string
		wantErr bool
	}{
		{name: "maximum number of ids", give: listOf(idListMaxLen)},
		{name: "too many ids", give: listOf(idListMaxLen + 1), wantErr: true},
		{name: "repeated id", give: testCmdBackupID + "," + testOtherJobID + "," + testCmdBackupID, wantErr: true},
		{name: "invalid id", give: testCmdBackupID + ",a/b", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := testMinimalRestore("")
			r.BackupIDs = tt.give

			got, err := cmds.serverRestore(r, testCmdJobID)
			if tt.wantErr {
				require.ErrorIs(t, err, errInvalidCmdParam)
				require.ErrorContains(t, err, paramBackupIDs)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, got, paramBackupIDs+"="+tt.give)
		})
	}
}
