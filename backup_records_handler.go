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
	"github.com/aerospike/backup-go/encoding"
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

func (bh *backupRecordsHandler) run(ctx context.Context, writers []*writeWorker[*models.Token], recordsTotal *atomic.Uint64) error {
	readWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)
	processorWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)

	partitionRanges, err := splitPartitions(
		bh.config.Partitions.Begin,
		bh.config.Partitions.Count,
		bh.config.Parallel,
	)
	if err != nil {
		return err
	}

	scanPolicy := *bh.config.ScanPolicy

	// if we are using the asb encoder, we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	if _, ok := bh.config.EncoderFactory.(*encoding.ASBEncoderFactory); ok {
		scanPolicy.RawCDT = true
	}

	for i := 0; i < bh.config.Parallel; i++ {
		ARRCFG := newArrConfig(bh.config, partitionRanges[i])

		recordReader := newAerospikeRecordReader(
			bh.aerospikeClient,
			ARRCFG,
			&scanPolicy,
			bh.logger,
		)

		readWorkers[i] = newReadWorker[*models.Token](recordReader)
		processorWorkers[i] = newProcessorWorker[*models.Token](newProcessorVoidTime(bh.logger))
	}

	writeWorkers := make([]pipeline.Worker[*models.Token], len(writers))

	for i, w := range writers {
		writeWorkers[i] = w
	}

	recordCounter := newTokenWorker(newRecordCounter(recordsTotal))

	job := pipeline.NewPipeline[*models.Token](
		readWorkers,
		recordCounter,
		processorWorkers,
		writeWorkers,
	)

	return job.Run(ctx)
}
