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
	"fmt"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// udfGetter is an interface for getting UDFs.
//
//go:generate mockery --name udfGetter
type udfGetter interface {
	GetUDFs(ctx context.Context) ([]*models.UDF, error)
}

// UdfReader satisfies the DataReader interface.
// It reads UDFs from a UDFGetter and returns them as *models.UDF.
type UdfReader struct {
	client udfGetter
	udfs   chan *models.UDF
	logger *slog.Logger
}

// NewUDFReader creates a new UdfReader.
func NewUDFReader(client udfGetter, logger *slog.Logger) *UdfReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeUDF)
	logger.Debug("created new udf reader")

	return &UdfReader{
		client: client,
		logger: logger,
	}
}

// Read reads the next UDF from the UDFGetter.
func (r *UdfReader) Read(ctx context.Context) (*models.Token, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// grab all the UDFs on the first run
	if r.udfs == nil {
		r.logger.Debug("fetching all UDFs")

		udfs, err := r.client.GetUDFs(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch UDFs: %w", err)
		}

		r.udfs = make(chan *models.UDF, len(udfs))
		for _, udf := range udfs {
			r.udfs <- udf
		}
	}

	if len(r.udfs) > 0 {
		UDFToken := models.NewUDFToken(<-r.udfs, 0)
		return UDFToken, nil
	}

	return nil, io.EOF
}

// Close satisfies the DataReader interface
// but is a no-op for the UDFReader.
func (r *UdfReader) Close() {}
