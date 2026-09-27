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
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/aerospike/backup-go/errclass"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// Fields of the server job status info responses.
const (
	fieldState = "state"
	fieldJobID = "job-id"
)

var (
	// restoreStartedStates are the restore states that only a job the server has
	// already accepted can be in.
	restoreStartedStates = []string{
		infomodels.RestoreStateRestoring,
		infomodels.RestoreStateFailed,
	}
)

// StartBackup starts a backup job on the server.
func (ic *Client) StartBackup(ctx context.Context, request *infomodels.RequestBackup) (string, error) {
	jobID := newJobID()

	cmd := fmt.Sprintf(ic.cmdDict[cmdIDServerBackup],
		request.Namespace,
		jobID,
		request.Storage,
		request.Bucket,
		request.Region,
		request.Profile,
		request.AccessKey,
		request.SecretKey,
		request.Endpoint,
		request.ModifiedBefore,
		request.ModifiedAfter,
		request.SetList,
		request.NoIndexes,
		request.NoUDFs,
		request.EnableChangeStream,
	)

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		principal, err := ic.getPrincipalNode()
		if err != nil {
			return fmt.Errorf("failed to get cluster principal: %w", err)
		}

		return ic.sendStartBackup(principal, cmd, jobID)
	})

	return jobID, err
}

// sendStartBackup sends the backup start command to node and reports whether the
// server accepted the job.
//
// The request can time out after the server has already accepted the job, so a
// failure is not conclusive on its own: the state of this very job id decides
// whether the command arrived. The status is looked up by job id, which is
// generated once per [Client.StartBackup] call, so state left by an earlier
// backup of the same namespace cannot be seen here.
func (ic *Client) sendStartBackup(node infoGetter, cmd, jobID string) error {
	_, startErr := ic.requestByNode(node, cmd)
	if startErr == nil {
		return nil
	}

	resp, statusErr := ic.getBackupStatusByNode(node, jobID)
	if statusErr != nil {
		return fmt.Errorf("failed start backup: %w (backup status unavailable: %w)",
			startErr, statusErr)
	}

	// Any state means the server got the command. What the job does next is
	// reported by [Client.GetBackupStatus] and is not decided here.
	if infomodels.NewResponseBackupState(resp) != nil {
		return nil
	}

	return fmt.Errorf("failed start backup: %w", startErr)
}

// AbortBackup aborts the backup job identified by backupID on the server.
func (ic *Client) AbortBackup(ctx context.Context, backupID string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDBackupAbort], backupID)

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed abort backup: %w", err)
		}

		return nil
	})
}

// AbortRestore aborts the restore job identified by backupID and namespace on the server.
func (ic *Client) AbortRestore(ctx context.Context, namespace, backupID string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDRestoreAbort], namespace, backupID)

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed abort restore: %w", err)
		}

		return nil
	})
}

// StartRestore starts a restore job on the server.
func (ic *Client) StartRestore(ctx context.Context, request *infomodels.RequestRestore) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDServerRestore],
		request.Namespace,
		request.JobID,
		request.Storage,
		request.Bucket,
		request.Region,
		request.Profile,
		request.AccessKey,
		request.SecretKey,
		request.Endpoint,
		request.FuzzyRestore,
		request.Path,
	)

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		principal, err := ic.getPrincipalNode()
		if err != nil {
			return fmt.Errorf("failed to get cluster principal: %w", err)
		}

		return ic.sendStartRestore(principal, cmd, request.Namespace, request.JobID)
	})
}

// sendStartRestore sends the restore start command to node and reports whether the
// server accepted the job.
//
// The request can time out after the server has already accepted the job, so a
// failure is not conclusive on its own: the state the server holds for this very
// job id decides whether the command arrived.
func (ic *Client) sendStartRestore(node infoGetter, cmd, namespace, jobID string) error {
	_, startErr := ic.requestByNode(node, cmd)
	if startErr == nil {
		return nil
	}

	resp, statusErr := ic.getRestoreStatusByNode(node, namespace)
	if statusErr != nil {
		return fmt.Errorf("failed start restore: %w (restore status unavailable: %w)",
			startErr, statusErr)
	}

	if isRestoreStarted(resp, jobID) {
		return nil
	}

	return fmt.Errorf("failed start restore: %w", startErr)
}

// isRestoreStarted reports whether the response confirms the server accepted the
// start command for the given restore job.
//
// The restore-status command is keyed by namespace, so the job id is matched to
// keep state left by an earlier restore of the same namespace from being taken
// for this one. The server reports a job id only while a job is present, and only
// the states in restoreStartedStates belong to a job that was actually started.
// What the job does after that is reported by [Client.GetRestoreStatus].
func isRestoreStarted(resp []infomodels.InfoMap, jobID string) bool {
	for _, r := range resp {
		if r[fieldJobID] != jobID {
			continue
		}

		if slices.Contains(restoreStartedStates, r[fieldState]) {
			return true
		}
	}

	return false
}

// PrepareRestore starts a restore preparation on the server.
func (ic *Client) PrepareRestore(ctx context.Context, jobID, namespace string) error {
	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		allNodes, err := ic.getNodesString()
		if err != nil {
			return fmt.Errorf("failed to get nodes string: %w", err)
		}

		cmd := fmt.Sprintf(ic.cmdDict[cmdIDServerPrepareRestore], namespace, jobID, allNodes)

		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed prepare restore: %w", err)
		}

		return nil
	})
}

func (ic *Client) getNodesString() (string, error) {
	nodes := ic.cluster.GetNodes()

	if len(nodes) == 0 {
		return "", fmt.Errorf("%w: no nodes available in cluster", errclass.ErrAerospike)
	}

	var builder strings.Builder

	for _, node := range nodes {
		if !node.IsActive() {
			continue
		}

		builder.WriteString(node.GetName())
		builder.WriteByte(',')
	}

	return builder.String(), nil
}

// GetBackupStatus aggregates server-side backup status across all nodes.
func (ic *Client) GetBackupStatus(ctx context.Context, jobID string) (*infomodels.ResponseBackupState, error) {
	return retryValue(ctx, ic.retryPolicy, func() (*infomodels.ResponseBackupState, error) {
		nodes := ic.cluster.GetNodes()

		statuses := make([]*infomodels.ResponseBackupState, 0, len(nodes))

		for _, node := range nodes {
			if !node.IsActive() {
				continue
			}

			resp, err := ic.getBackupStatusByNode(node, jobID)
			if err != nil {
				if strings.Contains(err.Error(), "no backup job") {
					return nil, ErrNotFound
				}

				return nil, fmt.Errorf("failed to get backup status from node %s: %w", node.GetName(), err)
			}

			if status := infomodels.NewResponseBackupState(resp); status != nil {
				statuses = append(statuses, status)
			}
		}

		if len(statuses) == 0 {
			return nil, fmt.Errorf("no backup state found for backup-id %s: %w", jobID, ErrNotFound)
		}

		return infomodels.MergeResponseBackupStates(statuses), nil
	})
}

func (ic *Client) getBackupStatusByNode(node infoGetter, jobID string) ([]infomodels.InfoMap, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDBackupStatus], jobID)

	result, err := ic.requestByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to request backup status: %w", err)
	}

	infoResponse, err := parseStdInfoResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse backup status: %w", err)
	}

	return infoResponse, nil
}

func (ic *Client) GetRestoreStatus(ctx context.Context, namespace string) (string, error) {
	return retryValue(ctx, ic.retryPolicy, func() (string, error) {
		nodes := ic.cluster.GetNodes()

		seen := make(map[string]struct{}, len(nodes))

		for _, node := range nodes {
			if !node.IsActive() {
				continue
			}

			resp, err := ic.getRestoreStatusByNode(node, namespace)
			if err != nil {
				return "", fmt.Errorf("failed to get restore status from node %s: %w", node.GetName(), err)
			}

			for _, r := range resp {
				state, ok := r[fieldState]
				if !ok {
					continue
				}

				seen[state] = struct{}{}
			}
		}

		if len(seen) == 0 {
			return "", fmt.Errorf("no restore state found for namespace %s: %w", namespace, ErrNotFound)
		}

		return infomodels.ResolveRestoreState(seen), nil
	})
}

func (ic *Client) getRestoreStatusByNode(node infoGetter, namespace string) ([]infomodels.InfoMap, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDRestoreStatus], namespace)

	result, err := ic.requestByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to request restore status: %w", err)
	}

	infoResponse, err := parseStdInfoResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse restore status: %w", err)
	}

	return infoResponse, nil
}
