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

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/xdr"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"golang.org/x/sync/semaphore"
)

// recordReaderProcessorXDR configures and creates record readers pipelines for xdr.
type recordReaderProcessorXDR[T models.TokenConstraint] struct {
	xdrConfig *ConfigBackupXDR
	// add scanConfig in the future.
	aerospikeClient AerospikeClient
	infoClient      *asinfo.InfoClient
	state           *State
	scanLimiter     *semaphore.Weighted
	rpsCollector    *metrics.Collector

	logger *slog.Logger
}

// newRecordReaderProcessorXDR returns a new record reader processor.
func newRecordReaderProcessorXDR[T models.TokenConstraint](
	xdrConfig *ConfigBackupXDR,
	aerospikeClient AerospikeClient,
	infoClient *asinfo.InfoClient,
	state *State,
	scanLimiter *semaphore.Weighted,
	rpsCollector *metrics.Collector,
	logger *slog.Logger,
) *recordReaderProcessorXDR[T] {
	logger.Debug("created new records reader processor")

	return &recordReaderProcessorXDR[T]{
		xdrConfig:       xdrConfig,
		aerospikeClient: aerospikeClient,
		infoClient:      infoClient,
		scanLimiter:     scanLimiter,
		state:           state,
		rpsCollector:    rpsCollector,
		logger:          logger,
	}
}

// recordReaderConfigForXDR creates reader config for XDR.
func (rr *recordReaderProcessorXDR[T]) recordReaderConfigForXDR() *xdr.RecordReaderConfig {
	localHostPort := fmt.Sprintf("%s:%d", rr.xdrConfig.LocalAddress, rr.xdrConfig.LocalPort)
	localTCPAddr := fmt.Sprintf(":%d", rr.xdrConfig.LocalPort)

	tcpConfig := xdr.NewTCPConfig(
		localTCPAddr,
		rr.xdrConfig.TLSConfig,
		rr.xdrConfig.ReadTimeout,
		rr.xdrConfig.WriteTimeout,
		rr.xdrConfig.ResultQueueSize,
		rr.xdrConfig.AckQueueSize,
		rr.xdrConfig.MaxConnections,
		rr.rpsCollector,
	)

	return xdr.NewRecordReaderConfig(
		rr.xdrConfig.DC,
		rr.xdrConfig.Namespace,
		rr.xdrConfig.Rewind,
		localHostPort,
		tcpConfig,
		rr.xdrConfig.InfoPolingPeriod,
		rr.xdrConfig.StartTimeout,
		rr.xdrConfig.MaxThroughput,
		rr.xdrConfig.Forward,
	)
}

// newReadWorkersXDR returns an XDR reader worker. The XDR reader worker will always
// use *models.ASBXToken.
func (rr *recordReaderProcessorXDR[T]) newReadWorkersXDR(ctx context.Context,
) ([]pipe.Reader[*models.ASBXToken], error) {
	readerConfig := rr.recordReaderConfigForXDR()
	reader, err := xdr.NewRecordReader(ctx, rr.infoClient, readerConfig, rr.logger)

	if err != nil {
		return nil, fmt.Errorf("failed to create xdr reader: %w", err)
	}
	// For xdr we will have 1 worker always.
	return []pipe.Reader[*models.ASBXToken]{reader}, nil
}
