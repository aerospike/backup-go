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

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/models"
)

type sindexWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	logger      *slog.Logger
}

// writeSecondaryIndex writes a secondary index to Aerospike.
func (rw sindexWriter) writeSecondaryIndex(si *models.SIndex) error {
	var sIndexType a.IndexType

	switch si.Path.BinType {
	case models.NumericSIDataType:
		sIndexType = a.NUMERIC
	case models.StringSIDataType:
		sIndexType = a.STRING
	case models.BlobSIDataType:
		sIndexType = a.BLOB
	case models.GEO2DSphereSIDataType:
		sIndexType = a.GEO2DSPHERE
	default:
		return fmt.Errorf("invalid sindex bin type: %c", si.Path.BinType)
	}

	var sIndexCollectionType a.IndexCollectionType

	switch si.IndexType {
	case models.BinSIndex:
		sIndexCollectionType = a.ICT_DEFAULT
	case models.ListElementSIndex:
		sIndexCollectionType = a.ICT_LIST
	case models.MapKeySIndex:
		sIndexCollectionType = a.ICT_MAPKEYS
	case models.MapValueSIndex:
		sIndexCollectionType = a.ICT_MAPVALUES
	default:
		return fmt.Errorf("invalid sindex collection type: %c", si.IndexType)
	}

	var (
		ctx []*a.CDTContext
		exp *a.Expression
		err error
	)

	if si.Path.B64Context != "" {
		ctx, err = a.Base64ToCDTContext(si.Path.B64Context)
		if err != nil {
			return fmt.Errorf("error decoding sindex context %s: %w", si.Path.B64Context, err)
		}
	}

	if si.Expression != "" {
		exp, err = a.ExpFromBase64(si.Expression)
		if err != nil {
			return fmt.Errorf("error decoding sindex expression %s: %w", si.Expression, err)
		}
	}

	job, aErr := rw.createIndex(
		rw.writePolicy,
		si,
		sIndexType,
		sIndexCollectionType,
		exp,
		ctx...,
	)
	if aErr != nil {
		if aErr.Matches(atypes.INDEX_FOUND) {
			rw.logger.Debug("secondary index already exists, replacing it", slog.String("name", si.Name))

			fmt.Println("START DROPPPING INDEX:", si.Namespace, si.Set, si.Name)
			err := rw.asc.DropIndex(rw.writePolicy, si.Namespace, si.Set, si.Name)
			if err != nil {
				return fmt.Errorf("error dropping sindex %s: %w", si.Name, err)
			}
			fmt.Println("FINISH DROPPPING INDEX:", si.Name)

			job, err = rw.createIndex(
				rw.writePolicy,
				si,
				sIndexType,
				sIndexCollectionType,
				exp,
				ctx...,
			)
			if err != nil {
				return fmt.Errorf("error creating replacement sindex %s: %w", si.Name, err)
			}
		} else {
			return fmt.Errorf("error creating sindex %s: %w", si.Name, aErr)
		}
	}

	if job == nil {
		return fmt.Errorf("error creating sindex: job is nil")
	}

	errs := job.OnComplete()
	if errs == nil {
		return fmt.Errorf("error creating sindex: OnComplete returned nil channel")
	}

	err = <-errs
	if err != nil {
		return fmt.Errorf("error creating sindex %s: %w", si.Name, err)
	}

	rw.logger.Debug("created secondary index", slog.String("name", si.Name))

	return nil
}

func (rw sindexWriter) createIndex(
	wp *a.WritePolicy,
	si *models.SIndex,
	sIndexType a.IndexType,
	sIndexCollectionType a.IndexCollectionType,
	exp *a.Expression,
	ctx ...*a.CDTContext,
) (*a.IndexTask, a.Error) {
	if si.Expression != "" {
		return rw.asc.CreateIndexWithExpression(
			wp,
			si.Namespace,
			si.Set,
			si.Name,
			sIndexType,
			sIndexCollectionType,
			exp,
		)
	}

	return rw.asc.CreateComplexIndex(
		wp,
		si.Namespace,
		si.Set,
		si.Name,
		si.Path.BinName,
		sIndexType,
		sIndexCollectionType,
		ctx...,
	)
}
