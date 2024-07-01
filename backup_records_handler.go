// Copyright 2024-2024 Aerospike, Inc.
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
	"log/slog"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
)

type backupRecordsHandler struct {
	config          *BackupConfig
	aerospikeClient *a.Client
	logger          *slog.Logger
}

func newBackupRecordsHandler(config *BackupConfig, ac *a.Client, logger *slog.Logger) *backupRecordsHandler {
	logger.Debug("created new backup records handler")

	return &backupRecordsHandler{
		config:          config,
		aerospikeClient: ac,
		logger:          logger,
	}
}

func (bh *backupRecordsHandler) run(
	ctx context.Context,
	writers []pipeline.Worker[*models.Token],
	recordsTotal *atomic.Uint64,
) error {
	readWorkers, err := bh.makeAerospikeReadWorkers(bh.config.Parallel)
	if err != nil {
		return err
	}

	recordCounter := newTokenWorker(processors.NewRecordCounter(recordsTotal))
	voidTimeSetter := newTokenWorker(processors.NewVoidTimeSetter(bh.logger))

	job := pipeline.NewPipeline[*models.Token](
		readWorkers,
		recordCounter,
		voidTimeSetter,
		writers,
	)

	return job.Run(ctx)
}

func (bh *backupRecordsHandler) makeAerospikeReadWorkers(n int) ([]pipeline.Worker[*models.Token], error) {
	partitionRanges, err := splitPartitions(bh.config.Partitions.Begin, bh.config.Partitions.Count, n)
	if err != nil {
		return nil, err
	}

	scanPolicy := *bh.config.ScanPolicy

	// we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	scanPolicy.RawCDT = true

	readWorkers := make([]pipeline.Worker[*models.Token], n)

	for i := 0; i < n; i++ {
		recordReader := newAerospikeRecordReader(
			bh.aerospikeClient,
			newArrConfig(bh.config, partitionRanges[i]),
			&scanPolicy,
			bh.logger,
		)

		readWorkers[i] = pipeline.NewReadWorker[*models.Token](recordReader)
	}

	return readWorkers, nil
}
