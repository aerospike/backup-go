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
	"encoding/base64"
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
	afterDigest []byte
}

func newBackupRecordsHandler(
	config *BackupConfig,
	ac AerospikeClient,
	logger *slog.Logger,
	scanLimiter *semaphore.Weighted,
) (*backupRecordsHandler, error) {
	logger.Debug("created new backup records handler")

	h := &backupRecordsHandler{
		config:          config,
		aerospikeClient: ac,
		logger:          logger,
		scanLimiter:     scanLimiter,
	}

	// For resuming backup from config.AfterDigest, we have to get and check key info, then set additional params.
	if config.AfterDigest != "" {
		if err := h.setAfterDigest(); err != nil {
			return nil, err
		}
	}

	return h, nil
}

func (bh *backupRecordsHandler) run(
	ctx context.Context,
	writers []pipeline.Worker[*models.Token],
	recordsReadTotal *atomic.Uint64,
) error {
	readWorkers, err := bh.makeAerospikeReadWorkers(ctx, bh.config.ParallelRead)
	if err != nil {
		return err
	}

	composeProcessor := newTokenWorker(processors.NewComposeProcessor(
		processors.NewRecordCounter(recordsReadTotal),
		processors.NewVoidTimeSetter(bh.logger),
		processors.NewTPSLimiter[*models.Token](
			ctx, bh.config.RecordsPerSecond),
	))

	return pipeline.NewPipeline(readWorkers, composeProcessor, writers).Run(ctx)
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

	readerConfig := bh.recordReaderConfigForPartition(bh.config.Partitions, &scanPolicy, bh.afterDigest)
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

		// For the first partition in the list we start from digest if it is set.
		if bh.afterDigest != nil && i == 0 {
			recordReaderConfig = bh.recordReaderConfigForPartition(partitionRanges[i], &scanPolicy, bh.afterDigest)
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
	// We should pass digest as parameter here, for implementing adding digest to first partition logic.
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

func (bh *backupRecordsHandler) setAfterDigest() error {
	digestBytes, err := base64.StdEncoding.DecodeString(bh.config.AfterDigest)
	if err != nil {
		return fmt.Errorf("failed to decode after-digest: %w", err)
	}

	keyDigest, err := a.NewKeyWithDigest(bh.config.Namespace, "", "", digestBytes)
	if err != nil {
		return fmt.Errorf("failed to init key from digest: %w", err)
	}

	bh.config.Partitions.Begin = keyDigest.PartitionId()
	bh.config.Partitions.Count -= bh.config.Partitions.Begin
	bh.afterDigest = keyDigest.Digest()

	return nil
}
