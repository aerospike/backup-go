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
	"time"

	"github.com/aerospike/backup-go/models"
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

	err = startWithRetry(ctx, ic.retryPolicy, ic.getPrincipalNode,
		func(node infoGetter, attempts *startAttempts) error {
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
// command arrived, see [startAttempts.record]. The status is looked up by
// job id, which is generated once per [Client.StartBackup] call, so state left
// by an earlier backup of the same namespace cannot be seen here. statusCmd must
// be the backup-status command for that job id.
func (ic *Client) sendStartBackup(node infoGetter, cmd, statusCmd string, attempts *startAttempts) error {
	attempts.sending(time.Now())

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

// StartRestore starts a restore job on the server.
//
// The server refuses a job id used before, except for a repeat of a live cold
// restore with exactly the same parameters, which it accepts as a no-op.
func (ic *Client) StartRestore(ctx context.Context, request *infomodels.RequestRestore) error {
	cmd, err := ic.cmds.serverRestore(request)
	if err != nil {
		return fmt.Errorf("failed to build restore command: %w", err)
	}

	statusCmd, err := ic.cmds.restoreStatus(request.JobID)
	if err != nil {
		return fmt.Errorf("failed to build restore status command: %w", err)
	}

	return startWithRetry(ctx, ic.retryPolicy, ic.getPrincipalNode,
		func(node infoGetter, attempts *startAttempts) error {
			return ic.sendStartRestore(node, cmd, statusCmd, request, attempts)
		})
}

// startWithRetry sends a start command with send to the node returned by
// principal, under policy. Every attempt shares one startAttempts, so that an
// attempt with an unknown outcome is still taken into account by the later
// ones, see [startAttempts.record].
func startWithRetry(ctx context.Context, policy *models.RetryPolicy, principal func() (infoGetter, error),
	send func(node infoGetter, attempts *startAttempts) error,
) error {
	var attempts startAttempts

	return executeWithRetry(ctx, policy, func() error {
		node, err := principal()
		if err != nil {
			return fmt.Errorf("failed to get cluster principal: %w", err)
		}

		return send(node, &attempts)
	})
}

// sendStartRestore sends the restore start command to node and reports whether the
// server accepted the job.
//
// The request can time out after the server has already accepted the job, so a
// failure is not conclusive while this attempt or an earlier one may have
// reached the server: the state the server holds for the job id of request then
// decides whether the command arrived, see [startAttempts.record]. The job id
// is chosen by the caller, so a row of it only confirms the start if it is the
// row of this request, see [isRestoreStarted]. statusCmd must be the
// restore-status command for the job.
func (ic *Client) sendStartRestore(node infoGetter, cmd, statusCmd string,
	request *infomodels.RequestRestore, attempts *startAttempts,
) error {
	attempts.sending(time.Now())

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

	if isRestoreStarted(resp, request, attempts.firstSent) {
		return nil
	}

	return fmt.Errorf("failed to start restore: %w", startErr)
}

// startAttempts tracks the attempts of one start command across its retries.
type startAttempts struct {
	// firstSent is when the first attempt was sent.
	firstSent time.Time
	// unanswered is true once an attempt failed with an unknown outcome, such
	// as on a timeout: the server may have accepted the job.
	unanswered bool
}

// sending records that an attempt is sent at now.
func (a *startAttempts) sending(now time.Time) {
	if a.firstSent.IsZero() {
		a.firstSent = now
	}
}

// record records the failure err of an attempt and reports whether the server
// may have accepted the job, on this attempt or an earlier one. An answered
// error is conclusive for its own attempt, except the ones the server returns
// when the start may have taken effect, see [mayHaveTakenEffect]. So after a
// conclusive error, such as for a job id in use, the job may only exist if an
// earlier attempt had an unknown outcome.
func (a *startAttempts) record(err error) (mayBeAccepted bool) {
	if mayHaveTakenEffect(err) {
		a.unanswered = true
	}

	return a.unanswered
}

// startClockSkew is the clock difference between the client and the server
// nodes that [isRestoreStarted] tolerates.
const startClockSkew = time.Minute

// isRestoreStarted reports whether the response confirms the server accepted the
// start command of request, first sent at firstSent: the server reports a job
// only once its restore command was accepted, so a row of the job proves it.
//
// The caller chooses the job id, and the server keeps the row of a finished
// job, so the row must be the one of this request. It must match the job id,
// the namespace and the mode of request, and a finished row must have started
// after firstSent, give or take startClockSkew: a row left by an earlier job
// with the same job id is never taken for this one. What the job does after
// that is reported by [Client.GetRestoreStatus].
func isRestoreStarted(resp []infomodels.InfoMap, request *infomodels.RequestRestore, firstSent time.Time) bool {
	status := infomodels.NewResponseRestoreState(resp)
	if status == nil ||
		status.JobID != request.JobID ||
		status.Namespace != request.Namespace ||
		status.Kind != restoreKindOf(request.Policy) {
		return false
	}

	if !status.IsFinished() {
		return true
	}

	return !status.StartTime.IsZero() && !status.StartTime.Before(firstSent.Add(-startClockSkew))
}

// restoreKindOf returns the restore mode the server runs for policy: cold for
// the strict policy, its default, and fuzzy for the fuzzy one.
func restoreKindOf(policy infomodels.RestorePolicy) infomodels.RestoreKind {
	if policy == infomodels.RestorePolicyFuzzy {
		return infomodels.RestoreKindFuzzy
	}

	return infomodels.RestoreKindCold
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
