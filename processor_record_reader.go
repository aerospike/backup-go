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
	"log/slog"
	"time"

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/io/aerospike/xdr"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"golang.org/x/sync/semaphore"
)

// recordReaderProcessor configure and create record readers pipelines.
type recordReaderProcessor[T models.TokenConstraint] struct {
	xdrConfig *ConfigBackupXDR
	// add scanConfig in the future.
	aerospikeClient AerospikeClient
	infoClient      *asinfo.InfoClient
	state           *State
	scanLimiter     *semaphore.Weighted

	logger *slog.Logger
}

// newRecordReaderProcessor returns new record reader processor.
func newRecordReaderProcessor[T models.TokenConstraint](
	xdrConfig *ConfigBackupXDR,
	aerospikeClient AerospikeClient,
	infoClient *asinfo.InfoClient,
	state *State,
	scanLimiter *semaphore.Weighted,
	logger *slog.Logger,
) *recordReaderProcessor[T] {
	logger.Debug("created new records reader processor")

	return &recordReaderProcessor[T]{
		xdrConfig:       xdrConfig,
		aerospikeClient: aerospikeClient,
		infoClient:      infoClient,
		scanLimiter:     scanLimiter,
		state:           state,
		logger:          logger,
	}
}

// recordReaderConfigForXDR creates reader config for XDR.
func (rr *recordReaderProcessor[T]) recordReaderConfigForXDR() *xdr.RecordReaderConfig {
	localHostPort := fmt.Sprintf("%s:%d", rr.xdrConfig.LocalAddress, rr.xdrConfig.LocalPort)
	localTCPAddr := fmt.Sprintf(":%d", rr.xdrConfig.LocalPort)

	tcpConfig := xdr.NewTCPConfig(
		localTCPAddr,
		rr.xdrConfig.TLSConfig,
		rr.xdrConfig.ReadTimoutMilliseconds,
		rr.xdrConfig.WriteTimeoutMilliseconds,
		rr.xdrConfig.ResultQueueSize,
		rr.xdrConfig.AckQueueSize,
		rr.xdrConfig.MaxConnections,
	)

	infoPolingPeriod := time.Duration(rr.xdrConfig.InfoPolingPeriodMilliseconds) * time.Millisecond
	startTimeout := time.Duration(rr.xdrConfig.StartTimeoutMilliseconds) * time.Millisecond

	return xdr.NewRecordReaderConfig(
		rr.xdrConfig.DC,
		rr.xdrConfig.Namespace,
		rr.xdrConfig.Rewind,
		localHostPort,
		tcpConfig,
		infoPolingPeriod,
		startTimeout,
	)
}

// newReadWorkersXDR returns XDR reader worker. XDR reader worker will always use *models.ASBXToken.
func (rr *recordReaderProcessor[T]) newReadWorkersXDR(ctx context.Context,
) ([]pipeline.Worker[*models.ASBXToken], error) {
	// For xdr we will have 1 worker always.
	readWorkers := make([]pipeline.Worker[*models.ASBXToken], 1)

	readerConfig := rr.recordReaderConfigForXDR()
	reader, err := xdr.NewRecordReader(ctx, rr.infoClient, readerConfig, rr.logger)

	if err != nil {
		return nil, fmt.Errorf("failed to create xdr reader: %w", err)
	}

	readWorkers[0] = pipeline.NewReadWorker[*models.ASBXToken](reader)

	return readWorkers, nil
}
