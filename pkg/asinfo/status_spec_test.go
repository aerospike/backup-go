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
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The status records below are the examples of the server specification
// (backup-status and restore-status), joined into one line. The parts the
// specification elides with "..." are left out.
const (
	specJobFull     = "full-1104"
	specJobNightly  = "nightly-07"
	specJobIncr     = "incr-1105"
	specNamespace   = "test"
	specClusterID   = "8f3a6c21d47e09b5"
	specReasonAbort = "aborted by operator"
	// specSecretsNs is a namespace named as the word the secret agent syntax
	// uses: a status row with it must be split like any other row.
	specSecretsNs = "secrets"

	specBackupRunning = "job-id=full-1104:ns=test:state=BASE_SCAN_ACTIVE:backup-rps=0:backup-worker-threads=4:" +
		"backup-upload-threads=8:backup-s3-throughput-target-gbps=10:backup-policy=strict:" +
		"achieved-consistency=-:recs-backed-up=1250000:recs-base=1250000:recs-query-incr=0:" +
		"recs-change-incr=0:bytes-backed-up=1610612736:bytes-base=1610612736:bytes-query-incr=0:" +
		"bytes-change-incr=0:recs-filtered=0:recs-degenerate=0:recs-skipped-xdr-tomb=0:" +
		"recs-read-failed=0:partitions-flushed=512:partitions-owned=1366:partitions-scanned=530:" +
		"progress-pct=37.48:change-stream-active=true:start-time=2026-11-04 10.00.00.000 UTC:" +
		"finish-time=-:partitions-count-pending=0:count-read-failures=0:backpressure-active=false:" +
		"backpressure-activations=3:bytes-in-per-sec=220200960:bytes-out-per-sec=209715200:" +
		"upload-q-depth=5:upload-q-cap=16:buffers-in-use=12:total-buffers-allocated=24:" +
		"recs-per-sec=171000:buffers-scan-cap=20:last-lap-ms=412"
	specBackupDraining = "job-id=full-1104:ns=test:state=FINAL_DRAINING:progress-pct=95.00:" +
		"change-stream-active=true:start-time=2026-11-04 10.00.00.000 UTC:finish-time=-:" +
		"drain-blocked-migrations=37:partitions-count-pending=0:last-lap-ms=380"
	specBackupFallback = "job-id=nightly-07:ns=test:state=COMPLETE:backup-rps=0:backup-worker-threads=4:" +
		"backup-upload-threads=8:backup-s3-throughput-target-gbps=10:backup-policy=best-effort:" +
		"achieved-consistency=fuzzy:fallback-reason=node bb9020011ac4202 failed - <reason>:" +
		"progress-pct=100.00:change-stream-active=false:" +
		"start-time=2026-11-04 01.00.00.000 UTC:finish-time=2026-11-04 01.42.10.512 UTC:last-lap-ms=-"
	specBackupOperatorAbort = "job-id=full-1104:ns=test:state=ABORTED:achieved-consistency=-:" +
		"finish-time=2026-11-04 10.12.03.004 UTC:error-reason=aborted by operator:" +
		"abort-origin=operator:partitions-count-pending=0:last-lap-ms=-"
	specBackupServerAbort = "job-id=full-1104:ns=test:state=ABORTED:" +
		"error-reason=node bb9020011ac4202 failed+ <reason>:abort-origin=server"
	specBackupFailedStub = "job-id=b9:ns=(none):state=FAILED:backup-rps=0:backup-worker-threads=0:" +
		"backup-upload-threads=0:backup-s3-throughput-target-gbps=0:backup-policy=-:" +
		"achieved-consistency=-:start-time=-:error-reason=<reason>"
	specBackupSeveral = "job-id=incr-1105:ns=test:state=INCR_SCAN_ACTIVE;job-id=full-1104:ns=test:state=COMPLETE"

	specRestoreColdClear = "job-id=r1:ns=test:kind=cold:state=ACTIVE:restore-rps=0:restore-worker-threads=8:" +
		"restore-download-threads=8:ns-state=PREPARING:start-time=2026-11-04 12.00.00.000 UTC:" +
		"finish-time=-:joiner=false:placeholder=false:pids-in-scope=1366:progress-pct=0.00:" +
		"missing-layout-nodes=0:n-done-nodes=0:source-consistency=-:backup-id=incr-1106:" +
		"backup-entry=2:backup-entries=3:cluster-key=8f3a6c21d47e09b5:done=false:" +
		"clear=required:clear-zeroed-bytes=0:clear-ms=0:total=0:applied=0:deletes=0:" +
		"filtered=0:filtered-not-ours=0:failed=0:recs-per-sec=0:pending-q-depth=0:" +
		"pending-q-cap=16:last-lap-ms=250"
	specRestoreColdHeld = "job-id=r1:ns=test:kind=cold:state=ACTIVE:ns-state=PREPARING:pids-in-scope=1366:" +
		"held-reason=truncate-state-live:progress-pct=0.00"
	specRestoreColdTTL = "job-id=r3:ns=test:kind=cold:state=ACTIVE:restore-rps=0:restore-worker-threads=8:" +
		"restore-download-threads=8:ns-state=RESTORING:finish-time=-:" +
		"joiner=false:placeholder=false:pids-in-scope=1366:progress-pct=61.20:" +
		"missing-layout-nodes=0:n-done-nodes=0:source-consistency=strict:extra-ttl=86400:" +
		"ttl-extended=812000:ttl-clamped=0:cluster-key=8f3a6c21d47e09b5:done=false:clear=none:" +
		"clear-zeroed-bytes=0:clear-ms=0:total=1904331:applied=1203300:deletes=1200:" +
		"filtered=701031:filtered-not-ours=700950:failed=0:recs-per-sec=95000:" +
		"pending-q-depth=9:pending-q-cap=16:last-lap-ms=301"
	specRestoreColdAborted = "job-id=r1:ns=test:kind=cold:state=ABORTED:ns-state=FAILED:" +
		"last-lap-ms=-:error-reason=aborted by operator:abort-origin=operator"
	specRestoreFuzzy = "job-id=r2:ns=test:kind=fuzzy:state=ACTIVE:restore-rps=0:restore-worker-threads=8:" +
		"restore-download-threads=8:ns-state=NONE:start-time=2026-11-04 14.00.00.000 UTC:" +
		"finish-time=-:total=5400211:applied=5400100:deletes=310:filtered=100:failed=11:" +
		"recs-per-sec=120000:ignore-record-error=true:outstanding=480:max-inflight=500:" +
		"covered=900:cluster-wide=2700:progress-pct=65.90:source-consistency=strict:" +
		"backup-id=incr-1105:backup-entry=1:backup-entries=2:delete-absent=4:last-lap-ms=205"
	specRestoreNamespaceOnly = "ns=test:ns-state=NONE"
	// specRestoreSecretsNs is a row of the namespace specSecretsNs.
	specRestoreSecretsNs = "job-id=r5:ns=" + specSecretsNs + ":kind=cold:state=ACTIVE:ns-state=READY"
)

// TestParseStatusResponse_Backup feeds the backup-status examples of the
// server specification through the status parser into the backup state.
func TestParseStatusResponse_Backup(t *testing.T) {
	t.Parallel()

	startTime := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		want *infomodels.ResponseBackupState
		name string
		give string
	}{
		{
			name: "running strict backup",
			give: specBackupRunning,
			want: &infomodels.ResponseBackupState{
				StartTime:                    startTime,
				JobID:                        specJobFull,
				Namespace:                    specNamespace,
				State:                        infomodels.BackupStateBaseScanActive,
				BackupWorkerThreads:          4,
				BackupUploadThreads:          8,
				BackupS3ThroughputTargetGbps: 10,
				Policy:                       infomodels.BackupPolicyStrict,
				RecsBackedUp:                 1250000,
				RecsBase:                     1250000,
				BytesBackedUp:                1610612736,
				BytesBase:                    1610612736,
				PartitionsFlushed:            512,
				PartitionsOwned:              1366,
				PartitionsScanned:            530,
				ProgressPct:                  37.48,
				ChangeStreamActive:           true,
				BackpressureActivations:      3,
				BytesInPerSec:                220200960,
				BytesOutPerSec:               209715200,
				UploadQDepth:                 5,
				UploadQCap:                   16,
				BuffersInUse:                 12,
				TotalBuffersAllocated:        24,
				RecsPerSec:                   171000,
				BuffersScanCap:               20,
				LastLapMs:                    new(uint64(412)),
			},
		},
		{
			name: "draining blocked by migrations",
			give: specBackupDraining,
			want: &infomodels.ResponseBackupState{
				StartTime:              startTime,
				JobID:                  specJobFull,
				Namespace:              specNamespace,
				State:                  infomodels.BackupStateFinalDraining,
				ProgressPct:            95,
				ChangeStreamActive:     true,
				DrainBlockedMigrations: 37,
				LastLapMs:              new(uint64(380)),
			},
		},
		{
			name: "best effort backup fell back to fuzzy",
			give: specBackupFallback,
			want: &infomodels.ResponseBackupState{
				StartTime:                    time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC),
				FinishTime:                   time.Date(2026, 11, 4, 1, 42, 10, 512000000, time.UTC),
				JobID:                        specJobNightly,
				Namespace:                    specNamespace,
				State:                        infomodels.BackupStateComplete,
				BackupWorkerThreads:          4,
				BackupUploadThreads:          8,
				BackupS3ThroughputTargetGbps: 10,
				Policy:                       infomodels.BackupPolicyBestEffort,
				AchievedConsistency:          infomodels.ConsistencyFuzzy,
				FallbackReason:               "node bb9020011ac4202 failed - <reason>",
				ProgressPct:                  100,
			},
		},
		{
			name: "aborted by the operator",
			give: specBackupOperatorAbort,
			want: &infomodels.ResponseBackupState{
				FinishTime:  time.Date(2026, 11, 4, 10, 12, 3, 4000000, time.UTC),
				JobID:       specJobFull,
				Namespace:   specNamespace,
				State:       infomodels.BackupStateAborted,
				ErrorReason: specReasonAbort,
				AbortOrigin: infomodels.AbortOriginOperator,
			},
		},
		{
			name: "aborted because another node failed",
			give: specBackupServerAbort,
			want: &infomodels.ResponseBackupState{
				JobID:       specJobFull,
				Namespace:   specNamespace,
				State:       infomodels.BackupStateAborted,
				ErrorReason: "node bb9020011ac4202 failed+ <reason>",
				AbortOrigin: infomodels.AbortOriginServer,
			},
		},
		{
			name: "failed stub",
			give: specBackupFailedStub,
			want: &infomodels.ResponseBackupState{
				JobID:       "b9",
				Namespace:   "(none)",
				State:       infomodels.BackupStateFailed,
				ErrorReason: "<reason>",
			},
		},
		{
			name: "several jobs, the first one is taken",
			give: specBackupSeveral,
			want: &infomodels.ResponseBackupState{
				JobID:     specJobIncr,
				Namespace: specNamespace,
				State:     infomodels.BackupStateIncrScanActive,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := parseStatusResponse(tt.give)
			require.NoError(t, err)

			assert.Equal(t, tt.want, infomodels.NewResponseBackupState(resp))
		})
	}
}

// TestParseStatusResponse_Restore feeds the restore-status examples of the
// server specification through the status parser into the restore state.
func TestParseStatusResponse_Restore(t *testing.T) {
	t.Parallel()

	const (
		specJobCold    = "r1"
		specJobTTL     = "r3"
		specWorkers    = 8
		specPidsInPlay = 1366
	)

	tests := []struct {
		want *infomodels.ResponseRestoreState
		name string
		give string
	}{
		{
			name: "cold restore preparing, this node must be cleared",
			give: specRestoreColdClear,
			want: &infomodels.ResponseRestoreState{
				StartTime:              time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
				JobID:                  specJobCold,
				Namespace:              specNamespace,
				Kind:                   infomodels.RestoreKindCold,
				State:                  infomodels.RestoreStateActive,
				NsState:                infomodels.NamespaceStatePreparing,
				RestoreWorkerThreads:   specWorkers,
				RestoreDownloadThreads: specWorkers,
				BackupID:               "incr-1106",
				BackupEntry:            2,
				BackupEntries:          3,
				PidsInScope:            specPidsInPlay,
				ClusterKey:             specClusterID,
				Clear:                  infomodels.ClearStateRequired,
				PendingQCap:            16,
				LastLapMs:              new(uint64(250)),
			},
		},
		{
			name: "cold restore held by the truncate state",
			give: specRestoreColdHeld,
			want: &infomodels.ResponseRestoreState{
				JobID:       specJobCold,
				Namespace:   specNamespace,
				Kind:        infomodels.RestoreKindCold,
				State:       infomodels.RestoreStateActive,
				NsState:     infomodels.NamespaceStatePreparing,
				PidsInScope: specPidsInPlay,
				HeldReason:  "truncate-state-live",
			},
		},
		{
			name: "cold restore of a single backup with an extra TTL",
			give: specRestoreColdTTL,
			want: &infomodels.ResponseRestoreState{
				JobID:                  specJobTTL,
				Namespace:              specNamespace,
				Kind:                   infomodels.RestoreKindCold,
				State:                  infomodels.RestoreStateActive,
				NsState:                infomodels.NamespaceStateRestoring,
				RestoreWorkerThreads:   specWorkers,
				RestoreDownloadThreads: specWorkers,
				ExtraTTL:               86400,
				TTLExtended:            812000,
				SourceConsistency:      infomodels.ConsistencyStrict,
				RecsPerSec:             95000,
				LastLapMs:              new(uint64(301)),
				ProgressPct:            61.2,
				Total:                  1904331,
				Applied:                1203300,
				Deletes:                1200,
				Filtered:               701031,
				FilteredNotOurs:        700950,
				PidsInScope:            specPidsInPlay,
				ClusterKey:             specClusterID,
				Clear:                  infomodels.ClearStateNone,
				PendingQDepth:          9,
				PendingQCap:            16,
			},
		},
		{
			name: "cold restore aborted by the operator",
			give: specRestoreColdAborted,
			want: &infomodels.ResponseRestoreState{
				JobID:       specJobCold,
				Namespace:   specNamespace,
				Kind:        infomodels.RestoreKindCold,
				State:       infomodels.RestoreStateAborted,
				NsState:     infomodels.NamespaceStateFailed,
				ErrorReason: specReasonAbort,
				AbortOrigin: infomodels.AbortOriginOperator,
			},
		},
		{
			name: "fuzzy restore of a list of strict backups",
			give: specRestoreFuzzy,
			want: &infomodels.ResponseRestoreState{
				StartTime:              time.Date(2026, 11, 4, 14, 0, 0, 0, time.UTC),
				JobID:                  "r2",
				Namespace:              specNamespace,
				Kind:                   infomodels.RestoreKindFuzzy,
				State:                  infomodels.RestoreStateActive,
				NsState:                infomodels.NamespaceStateNone,
				RestoreWorkerThreads:   specWorkers,
				RestoreDownloadThreads: specWorkers,
				BackupID:               specJobIncr,
				BackupEntry:            1,
				BackupEntries:          2,
				SourceConsistency:      infomodels.ConsistencyStrict,
				RecsPerSec:             120000,
				LastLapMs:              new(uint64(205)),
				ProgressPct:            65.9,
				Total:                  5400211,
				Applied:                5400100,
				Deletes:                310,
				Filtered:               100,
				Failed:                 11,
				Outstanding:            480,
				MaxInflight:            500,
				IgnoreRecordError:      true,
				Covered:                900,
				ClusterWide:            2700,
				DeleteAbsent:           4,
			},
		},
		{
			name: "namespace record without a job",
			give: specRestoreNamespaceOnly,
			want: nil,
		},
		{
			name: "namespace named secrets",
			give: specRestoreSecretsNs,
			want: &infomodels.ResponseRestoreState{
				JobID:     "r5",
				Namespace: specSecretsNs,
				Kind:      infomodels.RestoreKindCold,
				State:     infomodels.RestoreStateActive,
				NsState:   infomodels.NamespaceStateReady,
			},
		},
		{
			name: "empty reply",
			give: "",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := parseStatusResponse(tt.give)
			require.NoError(t, err)

			assert.Equal(t, tt.want, infomodels.NewResponseRestoreState(resp))
		})
	}
}

func TestParseStatusResponse(t *testing.T) {
	t.Parallel()

	const (
		testKeyJobID = "job-id"
		testKeyState = "state"
		testJobA     = "a"
		testJobB     = "b"
		testActive   = "ACTIVE"
	)

	tests := []struct {
		name    string
		give    string
		want    []infomodels.InfoMap
		wantErr bool
	}{
		{name: "empty", give: "", want: nil},
		{name: "separator only", give: ";", want: nil},
		{
			name: "two records with a trailing separator",
			give: "job-id=a:state=ACTIVE;job-id=b:state=ACTIVE;",
			want: []infomodels.InfoMap{
				{testKeyJobID: testJobA, testKeyState: testActive},
				{testKeyJobID: testJobB, testKeyState: testActive},
			},
		},
		{
			name: "value split on the first equals sign",
			give: "job-id=a:filter=x=y",
			want: []infomodels.InfoMap{{testKeyJobID: testJobA, "filter": "x=y"}},
		},
		{
			name: "secret agent syntax is not special",
			give: "job-id=a:ns=" + specSecretsNs + ":state=ACTIVE",
			want: []infomodels.InfoMap{{testKeyJobID: testJobA, "ns": specSecretsNs, testKeyState: testActive}},
		},
		{name: "field without a value", give: "job-id=a:state", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseStatusResponse(tt.give)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
