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

	"github.com/aerospike/backup-go/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// StartBackup starts a backup job on the server and returns its job id, which
// is generated once per call. The job id is returned with every error after the
// command is built, but a failed start names a job that may exist only if the
// error wraps [ErrOutcomeUnknown].
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

	err = retryOnPrincipal(ctx, ic.retryPolicy, ic.getPrincipalNode,
		func(node infoGetter, attempts *retryAttempts) error {
			return ic.sendStartBackup(node, cmd, statusCmd, attempts)
		})

	return jobID, err
}

// sendStartBackup sends the backup start command to node and reports whether the
// server accepted the job.
//
// The request can time out after the server has already accepted the job, so a
// failure is not conclusive while this attempt or an earlier one may have
// reached the server: the state of this very job id then decides whether the
// command arrived, see [retryAttempts.record]. The status is looked up by
// job id, which is generated once per [Client.StartBackup] call, so state left
// by an earlier backup of the same namespace cannot be seen here. statusCmd must
// be the backup-status command for that job id.
func (ic *Client) sendStartBackup(node infoGetter, cmd, statusCmd string, attempts *retryAttempts) error {
	_, startErr := ic.requestByNode(node, cmd)
	if startErr == nil {
		return nil
	}

	if !attempts.record(startErr) {
		return fmt.Errorf("failed to start backup: %w", startErr)
	}

	resp, statusErr := ic.getBackupStatusByNode(node, statusCmd)
	if statusErr != nil {
		if isNotFoundResp(statusErr) {
			return fmt.Errorf("failed to start backup: %w", startErr)
		}

		return fmt.Errorf("failed to start backup: %w (backup status unavailable: %w)",
			startErr, statusErr)
	}

	// Any state means the server got the command. What the job does next is
	// reported by [Client.GetBackupStatus] and is not decided here.
	if infomodels.NewResponseBackupState(resp) != nil {
		return nil
	}

	return fmt.Errorf("failed to start backup: %w", startErr)
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

// StartRestore starts a restore job on the server and returns its job id,
// which is generated once per call. The job id is returned with every error
// after the command is built, but a failed start names a job that may exist
// only if the error wraps [ErrOutcomeUnknown].
func (ic *Client) StartRestore(ctx context.Context, request *infomodels.RequestRestore) (string, error) {
	jobID := newJobID()

	cmd, err := ic.cmds.serverRestore(request, jobID)
	if err != nil {
		return "", fmt.Errorf("failed to build restore command: %w", err)
	}

	statusCmd, err := ic.cmds.restoreStatus(jobID)
	if err != nil {
		return "", fmt.Errorf("failed to build restore status command: %w", err)
	}

	err = retryOnPrincipal(ctx, ic.retryPolicy, ic.getPrincipalNode,
		func(node infoGetter, attempts *retryAttempts) error {
			return ic.sendStartRestore(node, cmd, statusCmd, attempts)
		})

	return jobID, err
}

// retryOnPrincipal sends a command whose effect may outlive a failed attempt,
// such as a job start or a restore open, with send to the node returned by
// principal, under policy. Every attempt shares one retryAttempts, so that an
// attempt with an unknown outcome is still taken into account by the later
// ones, see [retryAttempts.record].
//
// A failure wraps [ErrOutcomeUnknown] if any attempt may have reached the
// server: the server may still apply such a command after it answered, so even
// a later lookup that finds no effect does not rule it out.
func retryOnPrincipal(ctx context.Context, policy *models.RetryPolicy, principal func() (infoGetter, error),
	send func(node infoGetter, attempts *retryAttempts) error,
) error {
	var attempts retryAttempts

	err := executeWithRetry(ctx, policy, func() error {
		node, err := principal()
		if err != nil {
			return fmt.Errorf("failed to get cluster principal: %w", err)
		}

		return send(node, &attempts)
	})
	if err != nil && attempts.unanswered {
		return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
	}

	return err
}

// sendStartRestore sends the restore start command to node and reports whether the
// server accepted the job.
//
// The request can time out after the server has already accepted the job, so a
// failure is not conclusive while this attempt or an earlier one may have
// reached the server: the state of this very job id then decides whether the
// command arrived, see [retryAttempts.record]. The status is looked up by
// job id, which is generated once per [Client.StartRestore] call, so state left
// by an earlier restore of the same namespace cannot be seen here. statusCmd
// must be the restore-status command for that job id.
func (ic *Client) sendStartRestore(node infoGetter, cmd, statusCmd string, attempts *retryAttempts) error {
	_, startErr := ic.requestByNode(node, cmd)
	if startErr == nil {
		return nil
	}

	if !attempts.record(startErr) {
		return fmt.Errorf("failed to start restore: %w", startErr)
	}

	resp, statusErr := ic.getRestoreStatusByNode(node, statusCmd)
	if statusErr != nil {
		if isNotFoundResp(statusErr) {
			return fmt.Errorf("failed to start restore: %w", startErr)
		}

		return fmt.Errorf("failed to start restore: %w (restore status unavailable: %w)",
			startErr, statusErr)
	}

	// Any state means the server got the command. What the job does next is
	// reported by [Client.GetRestoreStatus] and is not decided here.
	if infomodels.NewResponseRestoreState(resp) != nil {
		return nil
	}

	return fmt.Errorf("failed to start restore: %w", startErr)
}

// retryAttempts tracks the attempts of one command across its retries, for a
// command whose effect may outlive a failed attempt, see [retryOnPrincipal].
type retryAttempts struct {
	// unanswered is true once an attempt failed with an unknown outcome, such
	// as on a timeout: the server may have applied the command.
	unanswered bool
}

// record records the failure err of an attempt and reports whether the server
// may have applied the command, on this attempt or an earlier one. An answered
// error is conclusive for its own attempt, except the ones the server returns
// when the command may have taken effect, see [mayHaveTakenEffect]. So after a
// conclusive error, such as for a job id in use, the command may only have
// taken effect if an earlier attempt had an unknown outcome.
func (a *retryAttempts) record(err error) (mayHaveApplied bool) {
	if mayHaveTakenEffect(err) {
		a.unanswered = true
	}

	return a.unanswered
}

// GetBackupStatus aggregates server-side backup status across all nodes. The
// job is not found if any active node does not know it or reports no row of
// it: the merged state is only correct with the row of every node.
func (ic *Client) GetBackupStatus(ctx context.Context, jobID string) (*infomodels.ResponseBackupState, error) {
	cmd, err := ic.cmds.backupStatus(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to build backup status command: %w", err)
	}

	return retryValue(ctx, ic.retryPolicy, func() (*infomodels.ResponseBackupState, error) {
		return getBackupStatus(ic, ic.cluster.GetNodes(), cmd, jobID)
	})
}

// getBackupStatus reads the backup status with cmd from every active node and
// merges the rows of the job jobID, see [Client.GetBackupStatus].
func getBackupStatus[N statusNode](ic *Client, nodes []N, cmd, jobID string,
) (*infomodels.ResponseBackupState, error) {
	statuses := make([]*infomodels.ResponseBackupState, 0, len(nodes))

	for _, node := range nodes {
		if !node.IsActive() {
			continue
		}

		status, err := ic.backupStatusOfNode(node, cmd, jobID)
		if err != nil {
			return nil, err
		}

		if status == nil {
			return nil, jobNotOnNodeErr(cmdNameBackup, jobID, node.GetName())
		}

		statuses = append(statuses, status)
	}

	if len(statuses) == 0 {
		return nil, fmt.Errorf("no backup state found for backup-id %s: %w", jobID, ErrNotFound)
	}

	return infomodels.MergeResponseBackupStates(statuses), nil
}

// backupStatusOfNode reads the row of the backup job jobID from node with the
// backup-status command cmd. It returns nil if the node reports no row, and an
// error wrapping ErrNotFound if the node does not know the job.
func (ic *Client) backupStatusOfNode(node statusNode, cmd, jobID string,
) (*infomodels.ResponseBackupState, error) {
	resp, err := ic.getBackupStatusByNode(node, cmd)
	if err != nil {
		if isNotFoundResp(err) {
			return nil, jobNotOnNodeErr(cmdNameBackup, jobID, node.GetName())
		}

		return nil, fmt.Errorf("failed to get backup status from node %s: %w", node.GetName(), err)
	}

	return infomodels.NewResponseBackupState(resp), nil
}

func (ic *Client) getBackupStatusByNode(node infoGetter, cmd string) ([]infomodels.InfoMap, error) {
	result, err := ic.requestByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to request backup status: %w", err)
	}

	infoResponse, err := parseStatusResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse backup status: %w", err)
	}

	return infoResponse, nil
}

// OpenRestore releases namespace held by a failed or aborted cold restore on
// the server, opening it to clients with the data the restore had written.
// A retry answered as not found after an attempt with an unknown outcome is
// taken as success, see [Client.sendOpenRestore]. A failure wraps
// [ErrOutcomeUnknown] if the namespace may have been opened anyway.
func (ic *Client) OpenRestore(ctx context.Context, namespace string) error {
	cmd, err := ic.cmds.restoreOpen(namespace)
	if err != nil {
		return fmt.Errorf("failed to build restore open command: %w", err)
	}

	return retryOnPrincipal(ctx, ic.retryPolicy, ic.getPrincipalNode,
		func(node infoGetter, attempts *retryAttempts) error {
			return ic.sendOpenRestore(node, cmd, attempts)
		})
}

// sendOpenRestore sends the restore open command cmd to node and reports
// whether the namespace is open.
//
// The server answers not found when nothing gates the namespace, which is also
// its answer to a repeat of an open that succeeded. So a not found answer means
// the namespace is open if an earlier attempt of the same call may have reached
// the server, see [retryAttempts.record]: the attempt timed out, went
// unanswered, or the server could not confirm that every node acknowledged it.
// Otherwise it is returned as an error, since nothing was there to open.
//
// An unavailable answer is conclusive: either nothing was done (SMD not settled
// yet) or only some keys were deleted (more keys than one command takes), and
// the retry deletes the rest. A not found answer after it means that the gating
// node of a remaining key has not reported it yet, so the namespace may still
// be gated on that node.
func (ic *Client) sendOpenRestore(node infoGetter, cmd string, attempts *retryAttempts) error {
	_, err := ic.requestByNode(node, cmd)
	if err == nil {
		return nil
	}

	// A not found answer is conclusive itself, so record reports only whether
	// an earlier attempt may have taken effect.
	if attempts.record(err) && isNotFoundResp(err) {
		return nil
	}

	return fmt.Errorf("failed to open restore: %w", err)
}

// GetRestoreStatus aggregates the status of the restore job jobID of namespace
// across all nodes. NsState of the result is READY only when every node that
// reports more than a stub row reports READY. A job of another namespace is not
// found, and so is a job that any active node does not know or reports no row
// of.
func (ic *Client) GetRestoreStatus(ctx context.Context, namespace, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	if err := ic.cmds.validateRestoreStatusNamespace(namespace); err != nil {
		return nil, fmt.Errorf("failed to validate restore status namespace: %w", err)
	}

	cmd, err := ic.cmds.restoreStatus(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to build restore status command: %w", err)
	}

	return retryValue(ctx, ic.retryPolicy, func() (*infomodels.ResponseRestoreState, error) {
		return getRestoreStatus(ic, ic.cluster.GetNodes(), cmd, namespace, jobID)
	})
}

// statusNode is the part of a cluster node the status and cluster-stable commands use.
type statusNode interface {
	infoGetter
	IsActive() bool
	GetName() string
}

// getRestoreStatus reads the restore status with cmd from every active node and
// merges the rows of the job jobID of namespace, see [Client.GetRestoreStatus].
// Unlike clearRestoreStorage, it stops at the first failing node: the merged
// state is only correct with the row of every node, and a partial merge could
// report READY or COMPLETE while the missing node is not. For the same reason
// a stub row, which names no namespace, is merged too. A job id is single-use,
// so a row of another namespace means the job is not one of namespace.
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

		if status == nil {
			return nil, jobNotOnNodeErr(cmdNameRestore, jobID, node.GetName())
		}

		if status.HasNamespace() && status.Namespace != namespace {
			return nil, noRestoreStateErr(namespace, jobID)
		}

		statuses = append(statuses, status)
	}

	if len(statuses) == 0 {
		return nil, noRestoreStateErr(namespace, jobID)
	}

	return infomodels.MergeResponseRestoreStates(statuses), nil
}

// restoreStatusOfNode reads the row of the restore job jobID from node with the
// restore-status command cmd. It returns nil if the node reports no row, and an
// error wrapping ErrNotFound if the node does not know the job.
func (ic *Client) restoreStatusOfNode(node statusNode, cmd, jobID string,
) (*infomodels.ResponseRestoreState, error) {
	resp, err := ic.getRestoreStatusByNode(node, cmd)
	if err != nil {
		if isNotFoundResp(err) {
			return nil, jobNotOnNodeErr(cmdNameRestore, jobID, node.GetName())
		}

		return nil, fmt.Errorf("failed to get restore status from node %s: %w", node.GetName(), err)
	}

	return infomodels.NewResponseRestoreState(resp), nil
}

// jobNotOnNodeErr reports that node does not know the job jobID of the command
// cmdName, backup or restore, or reports no row of it.
func jobNotOnNodeErr(cmdName, jobID, node string) error {
	return fmt.Errorf("%s job %s on node %s: %w", cmdName, jobID, node, ErrNotFound)
}

// noRestoreStateErr reports that no node holds a row of the restore job jobID
// of namespace.
func noRestoreStateErr(namespace, jobID string) error {
	return fmt.Errorf("no restore state found for job-id %s of namespace %s: %w",
		jobID, namespace, ErrNotFound)
}

// ClearRestoreStorage clears the storage of namespace on every node that the
// cold restore job jobID, while it prepares the namespace, reports as needing a
// clear, so that the namespace can reach READY there without a node restart.
// The server starts the restore once every node is READY.
//
// Unlike the other restore commands, restore-clear-storage applies to the node
// it is sent to, so it is sent to each such node instead of the principal. The
// clear runs in the background: its progress is reported by
// [Client.GetRestoreStatus]. A node that needs no clear, or is already
// clearing, is skipped, so a retry never sends a clear to a node twice. A node
// may back out of an accepted clear and report [infomodels.ClearStateRequired]
// again; the next pass sends the clear to it once more, whether it is a retry
// of this call or another call of ClearRestoreStorage.
//
// A stub row, which names no namespace, never needs a clear and is skipped,
// like the row of another namespace. A node that does not know the job does
// not stop the clear of the others, but its error, which wraps [ErrNotFound],
// is returned and the call is retried.
//
// The clear drops the data of the namespace on the node. The server accepts it
// only for a namespace with storage-engine device, and from a user with the
// restore permission and the truncate permission on the namespace. It refuses
// it otherwise, and the refusal is retried like any other error, although a
// retry cannot change it. On a strong consistency namespace, every node must be
// cleared with no cluster change between the first clear and the last.
func (ic *Client) ClearRestoreStorage(ctx context.Context, namespace, jobID string) error {
	statusCmd, err := ic.cmds.restoreStatus(jobID)
	if err != nil {
		return fmt.Errorf("failed to build restore status command: %w", err)
	}

	cmd, err := ic.cmds.restoreClearStorage(namespace)
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
// does not stop the others: the errors of all nodes are returned together, the
// ErrNotFound of a node that does not know the job included. The job is
// reported as not found by noRestoreStateErr only when no node failed and none
// reports a row of it.
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
		return noRestoreStateErr(namespace, jobID)
	}

	return errors.Join(errs...)
}

func (ic *Client) getRestoreStatusByNode(node infoGetter, cmd string) ([]infomodels.InfoMap, error) {
	result, err := ic.requestByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to request restore status: %w", err)
	}

	infoResponse, err := parseStatusResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse restore status: %w", err)
	}

	return infoResponse, nil
}
