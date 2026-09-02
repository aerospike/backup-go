// Copyright 2024 Aerospike, Inc.
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

package backup

import (
	"context"

	"github.com/aerospike/backup-go/models"
	infoModels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// ClusterInfo provides cluster metadata and introspection for client-side backup and restore.
type ClusterInfo interface {
	GetRecordCount(ctx context.Context, namespace string, sets []string) (uint64, error)
	GetRackNodes(ctx context.Context, rackID int) ([]string, error)
	GetService(ctx context.Context, node string) (string, error)
	GetVersion(ctx context.Context) (infoModels.AerospikeVersion, error)
	GetSIndexes(ctx context.Context, namespace string) ([]*models.SIndex, error)
	GetUDFs(ctx context.Context) ([]*models.UDF, error)
	SupportsBatchWrite(ctx context.Context) (bool, error)
	GetSetsList(ctx context.Context, namespace string) ([]string, error)
	GetNamespacesList(ctx context.Context) ([]string, error)
	GetStatus(ctx context.Context) (string, error)
	GetSIndexInfo(ctx context.Context, namespace string) (models.SIndexInfo, error)
	GetPrimaryPartitions(ctx context.Context, node, namespace string) ([]int, error)
	GetPendingMigrations(ctx context.Context, namespace string) (uint64, error)
	GetClusterStable(ctx context.Context, namespace string) (bool, error)
}

// ServerBackupInfo provides server-side backup and restore job control.
type ServerBackupInfo interface {
	StartServerBackup(ctx context.Context, request *infoModels.RequestBackup) (string, error)
	StartServerRestore(ctx context.Context, request *infoModels.RequestRestore) error
	PrepareServerRestore(ctx context.Context, jobID, namespace string) error
	GetBackupStatus(ctx context.Context, jobID string) (*infoModels.ResponseBackupState, error)
	GetRestoreStatus(ctx context.Context, namespace string) (string, error)
}
