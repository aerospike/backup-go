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
	"errors"
	"fmt"
	"strings"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// StartBackup starts a backup job on the server.
func (ic *Client) StartBackup(ctx context.Context, request *infomodels.RequestBackup) (string, error) {
	jobID := newJobID()

	cmd, err := ic.cmds.serverBackup(request, jobID)
	if err != nil {
		return "", fmt.Errorf("failed to build backup command: %w", err)
	}

	statusCmd, err := ic.cmds.backupStatus(jobID)
	if err != nil {
		return "", fmt.Errorf("failed to build backup status command: %w", err)
	}

	err = executeWithRetry(ctx, ic.retryPolicy, func() error {
		principal, err := ic.getPrincipalNode()
		if err != nil {
			return fmt.Errorf("failed to get cluster principal: %w", err)
		}

		return ic.sendStartBackup(principal, cmd, statusCmd)
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
// backup of the same namespace cannot be seen here. statusCmd must be the
// backup-status command for that job id.
func (ic *Client) sendStartBackup(node infoGetter, cmd, statusCmd string) error {
	_, startErr := ic.requestByNode(node, cmd)
	if startErr == nil {
		return nil
	}

	resp, statusErr := ic.getBackupStatusByNode(node, statusCmd)
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
	cmd, err := ic.cmds.backupAbort(backupID)
	if err != nil {
		return fmt.Errorf("failed to build backup abort command: %w", err)
	}

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed abort backup: %w", err)
		}

		return nil
	})
}

// AbortRestore aborts the restore job identified by backupID and namespace on the server.
func (ic *Client) AbortRestore(ctx context.Context, namespace, backupID string) error {
	cmd, err := ic.cmds.restoreAbort(namespace, backupID)
	if err != nil {
		return fmt.Errorf("failed to build restore abort command: %w", err)
	}

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed abort restore: %w", err)
		}

		return nil
	})
}

// StartRestore starts a restore job on the server.
func (ic *Client) StartRestore(ctx context.Context, request *infomodels.RequestRestore) error {
	cmd, err := ic.cmds.serverRestore(request)
	if err != nil {
		return fmt.Errorf("failed to build restore command: %w", err)
	}

	statusCmd, err := ic.cmds.restoreStatus(request.JobID)
	if err != nil {
		return fmt.Errorf("failed to build restore status command: %w", err)
	}

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		principal, err := ic.getPrincipalNode()
		if err != nil {
			return fmt.Errorf("failed to get cluster principal: %w", err)
		}

		return ic.sendStartRestore(principal, cmd, statusCmd, request.JobID)
	})
}

// sendStartRestore sends the restore start command to node and reports whether the
// server accepted the job.
//
// The request can time out after the server has already accepted the job, so a
// failure is not conclusive on its own: the state the server holds for this very
// job id decides whether the command arrived. statusCmd must be the
// restore-status command for the job.
func (ic *Client) sendStartRestore(node infoGetter, cmd, statusCmd, jobID string) error {
	_, startErr := ic.requestByNode(node, cmd)
	if startErr == nil {
		return nil
	}

	resp, statusErr := ic.getRestoreStatusByNode(node, statusCmd)
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
// A cold job is reported from its preparation on, so the presence of the job
// alone does not prove the start: see [infomodels.ResponseRestoreState.IsStarted].
// The job id is matched as well, so that a row of another job is never taken
// for this one. What the job does after that is reported by
// [Client.GetRestoreStatus].
func isRestoreStarted(resp []infomodels.InfoMap, jobID string) bool {
	status := infomodels.NewResponseRestoreState(resp)

	return status != nil && status.JobID == jobID && status.IsStarted()
}

// PrepareRestore starts a restore preparation on the server.
func (ic *Client) PrepareRestore(ctx context.Context, request *infomodels.RequestPrepareRestore) error {
	cmd, err := ic.cmds.serverPrepareRestore(request)
	if err != nil {
		return fmt.Errorf("failed to build prepare restore command: %w", err)
	}

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed prepare restore: %w", err)
		}

		return nil
	})
}

// GetBackupStatus aggregates server-side backup status across all nodes.
func (ic *Client) GetBackupStatus(ctx context.Context, jobID string) (*infomodels.ResponseBackupState, error) {
	cmd, err := ic.cmds.backupStatus(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to build backup status command: %w", err)
	}

	return retryValue(ctx, ic.retryPolicy, func() (*infomodels.ResponseBackupState, error) {
		nodes := ic.cluster.GetNodes()

		statuses := make([]*infomodels.ResponseBackupState, 0, len(nodes))

		for _, node := range nodes {
			if !node.IsActive() {
				continue
			}

			resp, err := ic.getBackupStatusByNode(node, cmd)
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

func (ic *Client) getBackupStatusByNode(node infoGetter, cmd string) ([]infomodels.InfoMap, error) {
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

// OpenRestore releases the namespace held by the aborted cold restore job jobID on
// the server, opening it to clients with the data the restore had written.
func (ic *Client) OpenRestore(ctx context.Context, namespace, jobID string) error {
	cmd, err := ic.cmds.restoreOpen(namespace, jobID)
	if err != nil {
		return fmt.Errorf("failed to build restore open command: %w", err)
	}

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		if err := ic.sendToPrincipal(cmd); err != nil {
			return fmt.Errorf("failed to open restore: %w", err)
		}

		return nil
	})
}

// GetRestoreStatus aggregates the status of the restore job jobID of namespace
// across all nodes. NsState of the result is READY only when every node hosting
// the namespace reports READY. A job of another namespace is not found.
func (ic *Client) GetRestoreStatus(ctx context.Context, namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	if err := ic.cmds.validateRestoreStatusNamespace(namespace); err != nil {
		return nil, fmt.Errorf("failed to build restore status command: %w", err)
	}

	cmd, err := ic.cmds.restoreStatus(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to build restore status command: %w", err)
	}

	return retryValue(ctx, ic.retryPolicy, func() (*infomodels.ResponseRestoreState, error) {
		return getRestoreStatus(ic, ic.cluster.GetNodes(), cmd, namespace, jobID)
	})
}

// statusNode is the part of a cluster node the status commands use.
type statusNode interface {
	infoGetter
	IsActive() bool
	GetName() string
}

// getRestoreStatus reads the restore status with cmd from every active node and
// merges the rows of the job jobID of namespace, see [Client.GetRestoreStatus].
// Unlike clearRestoreStorage, it stops at the first failing node: the merged
// state is only correct with the row of every node, and a partial merge could
// report READY or COMPLETE while the missing node is not.
func getRestoreStatus[N statusNode](ic *Client, nodes []N, cmd, namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	statuses := make([]*infomodels.ResponseRestoreState, 0, len(nodes))

	for _, node := range nodes {
		if !node.IsActive() {
			continue
		}

		status, err := ic.restoreStatusOfNode(node, cmd, jobID)
		if err != nil {
			return nil, err
		}

		if status != nil && status.Namespace == namespace {
			statuses = append(statuses, status)
		}
	}

	if len(statuses) == 0 {
		return nil, errNoRestoreState(namespace, jobID)
	}

	return infomodels.MergeResponseRestoreStates(statuses), nil
}

// restoreStatusOfNode reads the row of the restore job jobID from node with the
// restore-status command cmd. It returns nil if the node reports no row.
func (ic *Client) restoreStatusOfNode(node statusNode, cmd, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	resp, err := ic.getRestoreStatusByNode(node, cmd)
	if err != nil {
		if strings.Contains(err.Error(), "no restore job") {
			return nil, fmt.Errorf("restore job %s on node %s: %w", jobID, node.GetName(), ErrNotFound)
		}

		return nil, fmt.Errorf("failed to get restore status from node %s: %w", node.GetName(), err)
	}

	return infomodels.NewResponseRestoreState(resp), nil
}

// errNoRestoreState reports that no node holds a row of the restore job jobID
// of namespace.
func errNoRestoreState(namespace, jobID string) error {
	return fmt.Errorf("no restore state found for job-id %s of namespace %s: %w",
		jobID, namespace, ErrNotFound)
}

// ClearRestoreStorage clears the storage of namespace on every node that the
// cold restore job jobID, while it is being prepared, reports as needing a
// clear, so that the namespace can reach READY there without a node restart.
//
// Unlike the other restore commands, restore-clear-storage applies to the node
// it is sent to, so it is sent to each such node instead of the principal. The
// clear runs in the background: its progress is reported by
// [Client.GetRestoreStatus]. A node that needs no clear, or is already
// clearing, is skipped, so a retry never sends a clear to a node twice. A node
// may back out of an accepted clear and report [infomodels.ClearStateRequired]
// again; calling ClearRestoreStorage again sends the clear to it once more.
//
// The clear drops the data of the namespace on the node. The server accepts it
// only for an SSD namespace (storage-engine device) that is not in strong
// consistency, with sindex-gc-period not zero if the namespace has secondary
// indexes, and from a user with the restore permission and the truncate
// permission on the namespace. It refuses it otherwise, and the refusal is
// retried like any other error, although a retry cannot change it.
func (ic *Client) ClearRestoreStorage(ctx context.Context, namespace, jobID string) error {
	statusCmd, err := ic.cmds.restoreStatus(jobID)
	if err != nil {
		return fmt.Errorf("failed to build restore status command: %w", err)
	}

	cmd, err := ic.cmds.restoreClearStorage(namespace, jobID)
	if err != nil {
		return fmt.Errorf("failed to build restore clear storage command: %w", err)
	}

	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		return clearRestoreStorage(ic, ic.cluster.GetNodes(), statusCmd, cmd, namespace, jobID)
	})
}

// clearRestoreStorage sends the restore-clear-storage command cmd to every
// active node whose row of the restore job jobID of namespace, read with
// statusCmd, requires a clear, see [Client.ClearRestoreStorage]. A failing node
// does not stop the others: the errors of all nodes are returned together. The
// job is reported as not found only when no node failed, so that the errors of
// the nodes are never hidden behind ErrNotFound.
func clearRestoreStorage[N statusNode](ic *Client, nodes []N, statusCmd, cmd, namespace, jobID string) error {
	var (
		errs  []error
		found bool
	)

	for _, node := range nodes {
		if !node.IsActive() {
			continue
		}

		status, err := ic.restoreStatusOfNode(node, statusCmd, jobID)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		if status == nil || status.Namespace != namespace {
			continue
		}

		found = true

		if status.Clear != infomodels.ClearStateRequired {
			continue
		}

		if _, err := ic.requestByNode(node, cmd); err != nil {
			errs = append(errs, fmt.Errorf("failed to clear restore storage on node %s: %w",
				node.GetName(), err))
		}
	}

	if !found && len(errs) == 0 {
		return errNoRestoreState(namespace, jobID)
	}

	return errors.Join(errs...)
}

func (ic *Client) getRestoreStatusByNode(node infoGetter, cmd string) ([]infomodels.InfoMap, error) {
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
