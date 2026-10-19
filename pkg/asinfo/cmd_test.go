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
	testCmdBackupID  = "260901T000000-abcd"
	testCmdNodes     = "BB9020011AC4202,BB9030011AC4202"
	testCmdStorage   = "aws-s3"
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
	testCmdPrepareBase   = "restore-prepare:namespace=source-ns;job-id=260922T103653-5xsr"
	testCmdStorageParams = ";path=/backups/source-ns;s3-bucket=backup-bucket;s3-region=eu-central-1;" +
		"s3-profile=default;s3-endpoint=https://s3.example.com"
	testCmdNodesParam     = ";nodes=BB9020011AC4202,BB9030011AC4202"
	testCmdSetListParam   = ";set-list=set1,set2"
	testCmdFilterExpParam = ";filter-exp=kwGVfwIAAJMEDqNiaW4="

	testNameUnsetOmitted = "unset fields are omitted"

	// testCmdMissingNsAndJobID is how the errors report a missing namespace and job id.
	testCmdMissingNsAndJobID = "namespace, job-id"
)

// testPtr returns a pointer to v, for the optional fields of the requests.
func testPtr[T any](v T) *T {
	return &v
}

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

// testPrepareRestore returns a prepare restore request with the required fields set.
func testPrepareRestore() *infomodels.RequestPrepareRestore {
	return &infomodels.RequestPrepareRestore{Namespace: testCmdNamespace, JobID: testCmdJobID}
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
					JobID:     testCmdJobID,
					BackupIDs: testCmdBackupID,
				})
			},
			want: testCmdRestoreBase,
		},
		{
			name: cmdNameRestorePrepare,
			call: func(c infoCommands) (string, error) {
				return c.serverPrepareRestore(testPrepareRestore(), testCmdNodes)
			},
			want: testCmdPrepareBase + testCmdNodesParam,
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
				return c.restoreOpen(testCmdNamespace, testCmdJobID)
			},
			want: "restore-open:namespace=source-ns;job-id=260922T103653-5xsr",
		},
		{
			name: cmdNameRestoreClearStorage,
			call: func(c infoCommands) (string, error) {
				return c.restoreClearStorage(testCmdNamespace, testCmdJobID)
			},
			want: "restore-clear-storage:namespace=source-ns;job-id=260922T103653-5xsr",
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
				return cmds.serverRestore(&infomodels.RequestRestore{})
			},
			wantMissing: "namespace, job-id, backup-ids, object-storage-type",
		},
		{
			name: cmdNameRestorePrepare,
			call: func() (string, error) {
				return cmds.serverPrepareRestore(&infomodels.RequestPrepareRestore{}, "")
			},
			wantMissing: "namespace, job-id, nodes",
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
			call:        func() (string, error) { return cmds.restoreOpen("", "") },
			wantMissing: testCmdMissingNsAndJobID,
		},
		{
			name:        cmdNameRestoreClearStorage,
			call:        func() (string, error) { return cmds.restoreClearStorage("", "") },
			wantMissing: testCmdMissingNsAndJobID,
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
			call: func() (string, error) { return cmds.serverRestore(nil) },
		},
		{
			name: cmdNameRestorePrepare,
			call: func() (string, error) { return cmds.serverPrepareRestore(nil, testCmdNodes) },
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

func TestInfoCommands_ValidatePrepareRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		giveVersion infomodels.AerospikeVersion
		give        *infomodels.RequestPrepareRestore
		wantErr     error
	}{
		{
			name:        "valid",
			giveVersion: infomodels.AerospikeVersionSupportsIntegratedBackup,
			give:        testPrepareRestore(),
		},
		{
			name:        "server version too old",
			giveVersion: testVersionBeforeIntegratedBackup(),
			give:        testPrepareRestore(),
			wantErr:     errCommandNotSupported,
		},
		{
			name:        "nil request",
			giveVersion: infomodels.AerospikeVersionSupportsIntegratedBackup,
			wantErr:     errNilRequest,
		},
		{
			name:        "namespace missing",
			giveVersion: infomodels.AerospikeVersionSupportsIntegratedBackup,
			give:        &infomodels.RequestPrepareRestore{JobID: testCmdJobID},
			wantErr:     errMissingCmdParam,
		},
		{
			name:        "job id missing",
			giveVersion: infomodels.AerospikeVersionSupportsIntegratedBackup,
			give:        &infomodels.RequestPrepareRestore{Namespace: testCmdNamespace},
			wantErr:     errMissingCmdParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := newInfoCommands(tt.giveVersion).validatePrepareRestore(tt.give)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
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
			name:      "empty",
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
			JobID:     testCmdJobID,
			BackupIDs: backupIDs,
		})
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
			name:      "restore prepare job id with pipe",
			giveValue: testPipeValue,
			call: func(value string) (string, error) {
				return cmds.serverPrepareRestore(&infomodels.RequestPrepareRestore{
					Namespace: testCmdNamespace,
					JobID:     value,
				}, testCmdNodes)
			},
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
			name:      "restore open job id with pipe",
			giveValue: testPipeValue,
			call:      func(value string) (string, error) { return cmds.restoreOpen(testCmdNamespace, value) },
		},
		{
			name:      "restore clear storage job id with pipe",
			giveValue: testPipeValue,
			call: func(value string) (string, error) {
				return cmds.restoreClearStorage(testCmdNamespace, value)
			},
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
	allFields.NoIndexes = testPtr(true)
	allFields.NoUDFs = testPtr(false)

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
			},
			want: testCmdBackupBase + testCmdStorageParams + testCmdSetListParam + ";bin-list=bin1,bin2" +
				testModifiedAfterParam + ";modified-before=1700000100" + testCmdFilterExpParam +
				";no-indexes=true;no-udfs=false;backup-rps=1000;backup-worker-threads=8;" +
				"backup-upload-threads=16;backup-s3-throughput-target-gbps=25",
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
					NoUDFs:    testPtr(false),
				},
			},
			want: testCmdBackupBase + ";no-udfs=false",
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
		// testRestoreHead are the parameters of the requests below that precede
		// the mode flag, in the order the command adds them.
		testRestoreHead = testCmdRestoreBase + testCmdStorageParams + ";incremental-path=/backups/incr" +
			";no-indexes=false;no-udfs=true"
		// testRestoreLimits are the parameters of both modes that follow the filter.
		testRestoreLimits = ";restore-download-threads=8;restore-worker-threads=16;restore-rps=5000;" +
			"extra-ttl=3600"
		// testFuzzyParams are the fuzzy only parameters of the requests below.
		testFuzzyParams = ";allow-unhosted=true;max-inflight=400;retry-base-interval=250;" +
			"retry-multiplier=1.5;retry-max-attempts=3;ignore-record-error=false;apply-metadata-last=true"
		testFuzzyFlag = ";fuzzy-restore=true"
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	// newRequest returns a request with every parameter of both modes set.
	newRequest := func(fuzzy *bool) *infomodels.RequestRestore {
		common := testStorage()
		common.SetList = testCmdSetList
		common.NoIndexes = testPtr(false)
		common.NoUDFs = testPtr(true)

		return &infomodels.RequestRestore{
			RequestCommon:          common,
			JobID:                  testCmdJobID,
			BackupIDs:              testCmdBackupID,
			IncrementalPath:        testIncrementalPath,
			FuzzyRestore:           fuzzy,
			RestoreDownloadThreads: 8,
			RestoreWorkerThreads:   16,
			RestoreRPS:             5000,
			ExtraTTL:               3600,
		}
	}

	newFuzzyRequest := func() *infomodels.RequestRestore {
		r := newRequest(testPtr(true))
		r.AllowUnhosted = testPtr(true)
		r.MaxInflight = 400
		r.RetryBaseIntervalMs = testPtr(250)
		r.RetryMultiplier = 1.5
		r.RetryMaxAttempts = 3
		r.IgnoreRecordError = testPtr(false)
		r.ApplyMetadataLast = testPtr(true)

		return r
	}

	newColdRequest := func(fuzzy *bool) *infomodels.RequestRestore {
		r := newRequest(fuzzy)
		r.FilterExp = testCmdFilterExp

		return r
	}

	newMinimalFuzzyRequest := func() *infomodels.RequestRestore {
		return &infomodels.RequestRestore{
			RequestCommon: infomodels.RequestCommon{
				Namespace: testCmdNamespace,
				Storage:   testCmdStorage,
			},
			JobID:        testCmdJobID,
			BackupIDs:    testCmdBackupID,
			FuzzyRestore: testPtr(true),
		}
	}

	retryBaseIntervalZero := newMinimalFuzzyRequest()
	retryBaseIntervalZero.RetryBaseIntervalMs = testPtr(0)

	tests := []struct {
		name string
		give *infomodels.RequestRestore
		want string
	}{
		{
			name: "fuzzy restore sends every fuzzy parameter",
			give: newFuzzyRequest(),
			want: testRestoreHead + testFuzzyFlag + testCmdSetListParam + testRestoreLimits + testFuzzyParams,
		},
		{
			name: "cold restore sends the filter",
			give: newColdRequest(testPtr(false)),
			want: testRestoreHead + ";fuzzy-restore=false" + testCmdSetListParam + testCmdFilterExpParam +
				testRestoreLimits,
		},
		{
			name: "unset fuzzy restore is a cold restore",
			give: newColdRequest(nil),
			want: testRestoreHead + testCmdSetListParam + testCmdFilterExpParam + testRestoreLimits,
		},
		{
			name: testNameUnsetOmitted,
			give: newMinimalFuzzyRequest(),
			want: testCmdRestoreBase + testFuzzyFlag,
		},
		{
			name: "zero retry base interval is sent",
			give: retryBaseIntervalZero,
			want: testCmdRestoreBase + testFuzzyFlag + ";retry-base-interval=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverRestore(tt.give)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInfoCommands_ServerRestoreModeMismatch(t *testing.T) {
	t.Parallel()

	const (
		testColdMode  = "restore with fuzzy-restore=false: "
		testFuzzyMode = "restore with fuzzy-restore=true: "
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	newRequest := func(fuzzy *bool) *infomodels.RequestRestore {
		return &infomodels.RequestRestore{
			RequestCommon: infomodels.RequestCommon{
				Namespace: testCmdNamespace,
				Storage:   testCmdStorage,
			},
			JobID:        testCmdJobID,
			BackupIDs:    testCmdBackupID,
			FuzzyRestore: fuzzy,
		}
	}

	fuzzyWithFilter := newRequest(testPtr(true))
	fuzzyWithFilter.FilterExp = testCmdFilterExp

	coldWithFuzzyParams := newRequest(testPtr(false))
	coldWithFuzzyParams.AllowUnhosted = testPtr(false)
	coldWithFuzzyParams.MaxInflight = 1
	coldWithFuzzyParams.RetryBaseIntervalMs = testPtr(0)
	coldWithFuzzyParams.RetryMultiplier = 1
	coldWithFuzzyParams.RetryMaxAttempts = 1
	coldWithFuzzyParams.IgnoreRecordError = testPtr(false)
	coldWithFuzzyParams.ApplyMetadataLast = testPtr(false)

	unsetModeWithFuzzyParam := newRequest(nil)
	unsetModeWithFuzzyParam.MaxInflight = 1

	missingJobIDWithFuzzyParam := newRequest(nil)
	missingJobIDWithFuzzyParam.JobID = ""
	missingJobIDWithFuzzyParam.MaxInflight = 1

	tests := []struct {
		name        string
		give        *infomodels.RequestRestore
		wantErrText string
		wantMissing bool
	}{
		{
			name:        "missing parameter is reported with the mode mismatch",
			give:        missingJobIDWithFuzzyParam,
			wantErrText: testColdMode + paramMaxInflight,
			wantMissing: true,
		},
		{
			name:        "filter expression in fuzzy restore",
			give:        fuzzyWithFilter,
			wantErrText: testFuzzyMode + paramFilterExp,
		},
		{
			name: "every fuzzy only parameter in cold restore",
			give: coldWithFuzzyParams,
			wantErrText: testColdMode + "allow-unhosted, max-inflight, retry-base-interval, " +
				"retry-multiplier, retry-max-attempts, ignore-record-error, apply-metadata-last",
		},
		{
			name:        "fuzzy only parameter with unset mode",
			give:        unsetModeWithFuzzyParam,
			wantErrText: testColdMode + paramMaxInflight,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := cmds.serverRestore(tt.give)

			require.ErrorIs(t, err, errParamNotApplicable)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErrText)
			assert.Equal(t, tt.wantMissing, errors.Is(err, errMissingCmdParam))
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_ServerPrepareRestore(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name               string
		giveHydrateReplica *bool
		want               string
	}{
		{
			name: "hydrate replica unset is omitted",
			want: testCmdPrepareBase + testCmdNodesParam,
		},
		{
			name:               "hydrate replica true",
			giveHydrateReplica: testPtr(true),
			want:               testCmdPrepareBase + ";hydrate-replica=true" + testCmdNodesParam,
		},
		{
			name:               "hydrate replica false",
			giveHydrateReplica: testPtr(false),
			want:               testCmdPrepareBase + ";hydrate-replica=false" + testCmdNodesParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := testPrepareRestore()
			r.HydrateReplica = tt.giveHydrateReplica

			got, err := cmds.serverPrepareRestore(r, testCmdNodes)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
