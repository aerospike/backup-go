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

package aerospike

import (
	"fmt"
	"log/slog"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
	"golang.org/x/sync/semaphore"
)

// RecordReaderConfig represents the configuration for scanning Aerospike records.
type RecordReaderConfig struct {
	timeBounds      models.TimeBounds
	partitionFilter *a.PartitionFilter
	// If nodes are set we ignore partitionFilter.
	nodes       []*a.Node
	scanPolicy  *a.ScanPolicy
	scanLimiter *semaphore.Weighted
	namespace   string
	setList     []string
	binList     []string
	noTTLOnly   bool

	// throttler indicates that we should throttler the scan on NO_AVAILABLE_CONNECTIONS_TO_NODE error.
	throttler *ThrottleLimiter

	// pageSize used for paginated scan for saving reading state.
	// If pageSize = 0, we think that we use normal scan.
	pageSize int64

	rpsCollector *metrics.Collector
}

func (c *RecordReaderConfig) logAttrs() []any {
	var attrs []any

	if c.timeBounds.FromTime != nil {
		attrs = append(attrs, slog.Time("fromTime", *c.timeBounds.FromTime))
	}

	if c.timeBounds.ToTime != nil {
		attrs = append(attrs, slog.Time("toTime", *c.timeBounds.ToTime))
	}

	if c.partitionFilter != nil {
		attrs = append(attrs,
			slog.String("partitionFilter", fmt.Sprintf("%d-%d", c.partitionFilter.Begin, c.partitionFilter.Count)))
	}

	if len(c.nodes) > 0 {
		nodeStrs := make([]string, 0, len(c.nodes))

		for _, n := range c.nodes {
			if n != nil {
				nodeStrs = append(nodeStrs, n.String())
			}
		}

		attrs = append(attrs, slog.Any("nodes", nodeStrs))
	}

	if c.scanLimiter != nil {
		attrs = append(attrs, slog.Bool("scanLimiter", true))
	}

	if c.namespace != "" {
		attrs = append(attrs, slog.String("namespace", c.namespace))
	}

	if len(c.setList) > 0 {
		attrs = append(attrs, slog.Any("setList", c.setList))
	}

	if len(c.binList) > 0 {
		attrs = append(attrs, slog.Any("binList", c.binList))
	}

	if c.noTTLOnly {
		attrs = append(attrs, slog.Bool("noTTLOnly", true))
	}

	if c.pageSize > 0 {
		attrs = append(attrs, slog.Int64("pageSize", c.pageSize))
	}

	return attrs
}

// NewRecordReaderConfig creates a new RecordReaderConfig.
func NewRecordReaderConfig(
	namespace string,
	setList []string,
	partitionFilter *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
	binList []string,
	timeBounds models.TimeBounds,
	scanLimiter *semaphore.Weighted,
	noTTLOnly bool,
	pageSize int64,
	rpsCollector *metrics.Collector,
) *RecordReaderConfig {
	if len(setList) == 0 {
		setList = []string{""}
	}

	return &RecordReaderConfig{
		namespace:       namespace,
		setList:         setList,
		partitionFilter: partitionFilter,
		scanPolicy:      scanPolicy,
		binList:         binList,
		timeBounds:      timeBounds,
		scanLimiter:     scanLimiter,
		noTTLOnly:       noTTLOnly,
		pageSize:        pageSize,
		rpsCollector:    rpsCollector,
		throttler:       NewThrottleLimiter(8, 5*time.Second),
	}
}
