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
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"golang.org/x/sync/semaphore"
)

type backupRecordsHandler struct {
	config          *BackupConfig
	aerospikeClient AerospikeClient
	logger          *slog.Logger
	scanLimiter     *semaphore.Weighted
	// is used when AfterDigest is set.
	keyDigest *a.Key
}

func newBackupRecordsHandler(
	config *BackupConfig,
	ac AerospikeClient,
	logger *slog.Logger,
	scanLimiter *semaphore.Weighted,
) (*backupRecordsHandler, error) {
	logger.Debug("created new backup records handler")
	// For resuming backup from config.AfterDigest, we have to get key info.
	var (
		keyDigest *a.Key
		err       error
	)

	if config.AfterDigest != nil {
		keyDigest, err = a.NewKeyWithDigest(config.Namespace, "", "", config.AfterDigest)
		if err != nil {
			return nil, fmt.Errorf("failed to init key from digest: %w", err)
		}

		config.Partitions.Begin = keyDigest.PartitionId()
	}

	return &backupRecordsHandler{
		config:          config,
		aerospikeClient: ac,
		logger:          logger,
		scanLimiter:     scanLimiter,
		keyDigest:       keyDigest,
	}, nil
}

func (bh *backupRecordsHandler) run(
	ctx context.Context,
	writers []pipeline.Worker[*models.Token],
	recordsReadTotal *atomic.Uint64,
) error {
	readWorkers, err := bh.makeAerospikeReadWorkers(ctx, bh.config.Parallel)
	if err != nil {
		return err
	}

	recordCounter := newTokenWorker(processors.NewRecordCounter(recordsReadTotal))
	voidTimeSetter := newTokenWorker(processors.NewVoidTimeSetter(bh.logger))
	tpsLimiter := newTokenWorker(processors.NewTPSLimiter[*models.Token](
		ctx, bh.config.RecordsPerSecond))

	job := pipeline.NewPipeline[*models.Token](
		readWorkers,

		// in the pipeline, first all counters.
		recordCounter,

		// speed limiters.
		tpsLimiter,

		// modifications.
		voidTimeSetter,

		writers,
	)

	return job.Run(ctx)
}

func (bh *backupRecordsHandler) countRecords(ctx context.Context, infoClient *asinfo.InfoClient) (uint64, error) {
	if bh.config.isFullBackup() {
		return infoClient.GetRecordCount(bh.config.Namespace, bh.config.SetList)
	}

	return bh.countRecordsUsingScan(ctx)
}

func (bh *backupRecordsHandler) countRecordsUsingScan(ctx context.Context) (uint64, error) {
	scanPolicy := *bh.config.ScanPolicy

	scanPolicy.IncludeBinData = false
	scanPolicy.MaxRecords = 0

	if bh.keyDigest != nil {
		bh.config.Partitions.Begin = bh.keyDigest.PartitionId()
	}

	readerConfig := bh.recordReaderConfigForPartition(bh.config.Partitions, &scanPolicy, bh.keyDigest.Digest())
	recordReader := aerospike.NewRecordReader(ctx, bh.aerospikeClient, readerConfig, bh.logger)

	defer recordReader.Close()

	var count uint64

	for {
		if _, err := recordReader.Read(); err != nil {
			if err == io.EOF {
				break
			}

			return 0, fmt.Errorf("error during records counting: %w", err)
		}

		count++
	}

	return count, nil
}

func (bh *backupRecordsHandler) makeAerospikeReadWorkers(
	ctx context.Context, n int,
) ([]pipeline.Worker[*models.Token], error) {
	partitionRanges, err := splitPartitions(
		bh.config.Partitions.Begin,
		bh.config.Partitions.Count,
		n)
	if err != nil {
		return nil, err
	}

	scanPolicy := *bh.config.ScanPolicy

	// we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	scanPolicy.RawCDT = true

	readWorkers := make([]pipeline.Worker[*models.Token], n)

	for i := 0; i < n; i++ {
		recordReaderConfig := bh.recordReaderConfigForPartition(partitionRanges[i], &scanPolicy, nil)

		// For 1 partition in the list we start from digest if it is set.
		if bh.keyDigest != nil && i == 0 {
			recordReaderConfig = bh.recordReaderConfigForPartition(partitionRanges[i], &scanPolicy, bh.keyDigest.Digest())
		}

		recordReader := aerospike.NewRecordReader(
			ctx,
			bh.aerospikeClient,
			recordReaderConfig,
			bh.logger,
		)

		readWorkers[i] = pipeline.NewReadWorker[*models.Token](recordReader)
	}

	return readWorkers, nil
}

func (bh *backupRecordsHandler) recordReaderConfigForPartition(
	partitionRange PartitionRange,
	scanPolicy *a.ScanPolicy,
	digest []byte,
) *aerospike.RecordReaderConfig {
	partitionFilter := a.NewPartitionFilterByRange(partitionRange.Begin, partitionRange.Count)
	if digest != nil {
		partitionFilter.Digest = digest
	}

	return aerospike.NewRecordReaderConfig(
		bh.config.Namespace,
		bh.config.SetList,
		partitionFilter,
		scanPolicy,
		bh.config.BinList,
		models.TimeBounds{
			FromTime: bh.config.ModAfter,
			ToTime:   bh.config.ModBefore,
		},
		bh.scanLimiter,
	)
}
