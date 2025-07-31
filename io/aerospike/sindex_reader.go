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
	"context"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// sindexGetter is an interface for getting secondary indexes.
//
//go:generate mockery --name sindexGetter
type sindexGetter interface {
	GetSIndexes(namespace string) ([]*models.SIndex, error)
}

// SindexReader satisfies the DataReader interface.
// It reads secondary indexes from a SIndexGetter and returns them as *models.SecondaryIndex.
type SindexReader struct {
	client    sindexGetter
	sindexes  chan *models.SIndex
	logger    *slog.Logger
	namespace string
}

// NewSIndexReader creates a new SIndexReader.
func NewSIndexReader(client sindexGetter, namespace string, logger *slog.Logger) *SindexReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeSIndex)
	logger.Debug("created new sindex reader")

	return &SindexReader{
		client:    client,
		namespace: namespace,
		logger:    logger,
	}
}

// Read reads the next secondary index from the SIndexGetter.
func (r *SindexReader) Read(ctx context.Context) (*models.Token, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// grab all the sindexes on the first run
	if r.sindexes == nil {
		r.logger.Debug("fetching all secondary indexes")

		sindexes, err := r.client.GetSIndexes(r.namespace)
		if err != nil {
			return nil, err
		}

		r.sindexes = make(chan *models.SIndex, len(sindexes))

		for _, sindex := range sindexes {
			if sindex.Expression != "" {
				r.logger.Warn("skipping secondary index with expression",
					slog.String("namespace", sindex.Namespace),
					slog.String("set", sindex.Set),
					slog.String("name", sindex.Name),
					slog.String("expression", sindex.Expression),
				)
			}

			r.sindexes <- sindex
		}
	}

	if len(r.sindexes) > 0 {
		SIToken := models.NewSIndexToken(<-r.sindexes, 0)
		return SIToken, nil
	}

	return nil, io.EOF
}

// Close satisfies the DataReader interface
// but is a no-op for the SIndexReader.
func (r *SindexReader) Close() {}
