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
	testCmdNodes     = "BB9020011AC4202,BB9030011AC4202"
	testCmdStorage   = "aws-s3"
	testCmdBucket    = "backup-bucket"
	testCmdRegion    = "eu-central-1"
	testCmdProfile   = "default"
	testCmdAccessKey = "access-value-fake"
	testCmdSecretKey = "sensitive-value-fake"
	testCmdEndpoint  = "https://s3.example.com"
	testCmdPath      = "/backups/source-ns"

	testCmdBackupPrefix  = "backup:namespace=source-ns;job-id=260922T103653-5xsr;object-storage-type=aws-s3;"
	testCmdRestorePrefix = "restore:namespace=source-ns;job-id=260922T103653-5xsr;object-storage-type=aws-s3;"
	testCmdS3Params      = "s3-bucket=backup-bucket;s3-region=eu-central-1;s3-profile=default;" +
		"access-key=access-value-fake;secret-key=sensitive-value-fake;s3-endpoint=https://s3.example.com;"
	testCmdBackupFlagsOff = "no-indexes=false;no-udfs=false;enable-change-stream=false"
)

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
			want: testCmdBackupPrefix + testCmdBackupFlagsOff,
		},
		{
			name: cmdNameRestore,
			call: func(c infoCommands) (string, error) {
				return c.serverRestore(&infomodels.RequestRestore{
					RequestCommon: infomodels.RequestCommon{
						Namespace: testCmdNamespace,
						Storage:   testCmdStorage,
					},
					JobID: testCmdJobID,
				})
			},
			want: testCmdRestorePrefix + "fuzzy-restore=false",
		},
		{
			name: cmdNameRestorePrepare,
			call: func(c infoCommands) (string, error) {
				return c.serverPrepareRestore(testCmdNamespace, testCmdJobID, testCmdNodes)
			},
			want: "restore-prepare:namespace=source-ns;job-id=260922T103653-5xsr;" +
				"nodes=BB9020011AC4202,BB9030011AC4202",
		},
		{
			name: cmdNameBackupStatus,
			call: func(c infoCommands) (string, error) { return c.backupStatus(testCmdJobID) },
			want: "backup-status:job-id=260922T103653-5xsr",
		},
		{
			name: cmdNameRestoreStatus,
			call: func(c infoCommands) (string, error) { return c.restoreStatus(testCmdNamespace) },
			want: "restore-status:namespace=source-ns",
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

	const wantMissingStartParams = "namespace, job-id, object-storage-type"

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
			wantMissing: wantMissingStartParams,
		},
		{
			name: cmdNameRestore,
			call: func() (string, error) {
				return cmds.serverRestore(&infomodels.RequestRestore{})
			},
			wantMissing: wantMissingStartParams,
		},
		{
			name: cmdNameRestorePrepare,
			call: func() (string, error) {
				return cmds.serverPrepareRestore("", "", "")
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
			wantMissing: paramNamespace,
		},
		{
			name:        cmdNameBackupAbort,
			call:        func() (string, error) { return cmds.backupAbort("") },
			wantMissing: paramJobID,
		},
		{
			name:        cmdNameRestoreAbort,
			call:        func() (string, error) { return cmds.restoreAbort("", "") },
			wantMissing: "namespace, job-id",
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
		name          string
		giveVersion   infomodels.AerospikeVersion
		giveNamespace string
		giveJobID     string
		wantErr       error
	}{
		{
			name:          "valid",
			giveVersion:   infomodels.AerospikeVersionSupportsIntegratedBackup,
			giveNamespace: testCmdNamespace,
			giveJobID:     testCmdJobID,
		},
		{
			name:          "server version too old",
			giveVersion:   testVersionBeforeIntegratedBackup(),
			giveNamespace: testCmdNamespace,
			giveJobID:     testCmdJobID,
			wantErr:       errCommandNotSupported,
		},
		{
			name:        "namespace missing",
			giveVersion: infomodels.AerospikeVersionSupportsIntegratedBackup,
			giveJobID:   testCmdJobID,
			wantErr:     errMissingCmdParam,
		},
		{
			name:          "job id missing",
			giveVersion:   infomodels.AerospikeVersionSupportsIntegratedBackup,
			giveNamespace: testCmdNamespace,
			wantErr:       errMissingCmdParam,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := newInfoCommands(tt.giveVersion).validatePrepareRestore(tt.giveNamespace, tt.giveJobID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInfoCommands_InvalidParams(t *testing.T) {
	t.Parallel()

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name string
		call func() (string, error)
	}{
		{
			name: cmdNameBackup,
			call: func() (string, error) {
				return cmds.serverBackup(&infomodels.RequestBackup{
					RequestCommon: infomodels.RequestCommon{
						Namespace: testCmdNamespace,
						Storage:   testCmdStorage,
					},
					SetList: testUnsafeValue,
				}, testCmdJobID)
			},
		},
		{
			name: cmdNameRestore,
			call: func() (string, error) {
				return cmds.serverRestore(&infomodels.RequestRestore{
					RequestCommon: infomodels.RequestCommon{
						Namespace: testCmdNamespace,
						Storage:   testCmdStorage,
					},
					JobID: testCmdJobID,
					Path:  testUnsafeValue,
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()

			require.ErrorIs(t, err, errInvalidCmdParam)
			assert.NotContains(t, err.Error(), testUnsafeValue)
			assert.Empty(t, got)
		})
	}
}

func TestInfoCommands_ServerBackup(t *testing.T) {
	t.Parallel()

	const (
		testModifiedBefore = "1700000100"
		testModifiedAfter  = "1700000000"
		testSetList        = "set1,set2"
	)

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name string
		give *infomodels.RequestBackup
		want string
	}{
		{
			name: "all fields set",
			give: &infomodels.RequestBackup{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
					Bucket:    testCmdBucket,
					Region:    testCmdRegion,
					Profile:   testCmdProfile,
					AccessKey: testCmdAccessKey,
					SecretKey: testCmdSecretKey,
					Endpoint:  testCmdEndpoint,
				},
				ModifiedBefore:     testModifiedBefore,
				ModifiedAfter:      testModifiedAfter,
				SetList:            testSetList,
				NoIndexes:          true,
				NoUDFs:             true,
				EnableChangeStream: true,
			},
			want: testCmdBackupPrefix +
				testCmdS3Params + "modified-before=1700000100;modified-after=1700000000;" +
				"set-list=set1,set2;no-indexes=true;no-udfs=true;enable-change-stream=true",
		},
		{
			name: "empty fields are omitted",
			give: &infomodels.RequestBackup{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
					Bucket:    testCmdBucket,
					Region:    testCmdRegion,
				},
				ModifiedAfter: testModifiedAfter,
			},
			want: testCmdBackupPrefix +
				"s3-bucket=backup-bucket;s3-region=eu-central-1;modified-after=1700000000;" +
				testCmdBackupFlagsOff,
		},
		{
			name: "flags are sent independently",
			give: &infomodels.RequestBackup{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
				},
				NoUDFs: true,
			},
			want: testCmdBackupPrefix +
				"no-indexes=false;no-udfs=true;enable-change-stream=false",
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

	cmds := newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup)

	tests := []struct {
		name string
		give *infomodels.RequestRestore
		want string
	}{
		{
			name: "all fields set",
			give: &infomodels.RequestRestore{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
					Bucket:    testCmdBucket,
					Region:    testCmdRegion,
					Profile:   testCmdProfile,
					AccessKey: testCmdAccessKey,
					SecretKey: testCmdSecretKey,
					Endpoint:  testCmdEndpoint,
				},
				JobID:        testCmdJobID,
				Path:         testCmdPath,
				FuzzyRestore: true,
			},
			want: testCmdRestorePrefix + testCmdS3Params + "fuzzy-restore=true;path=/backups/source-ns",
		},
		{
			name: "empty fields are omitted and false flag is sent",
			give: &infomodels.RequestRestore{
				RequestCommon: infomodels.RequestCommon{
					Namespace: testCmdNamespace,
					Storage:   testCmdStorage,
					Bucket:    testCmdBucket,
				},
				JobID: testCmdJobID,
				Path:  testCmdPath,
			},
			want: testCmdRestorePrefix +
				"s3-bucket=backup-bucket;fuzzy-restore=false;path=/backups/source-ns",
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
