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
	var sindexType a.IndexType

	switch si.Path.BinType {
	case models.NumericSIDataType:
		sindexType = a.NUMERIC
	case models.StringSIDataType:
		sindexType = a.STRING
	case models.BlobSIDataType:
		sindexType = a.BLOB
	case models.GEO2DSphereSIDataType:
		sindexType = a.GEO2DSPHERE
	default:
		return fmt.Errorf("invalid sindex bin type: %c", si.Path.BinType)
	}

	var sindexCollectionType a.IndexCollectionType

	switch si.IndexType {
	case models.BinSIndex:
		sindexCollectionType = a.ICT_DEFAULT
	case models.ListElementSIndex:
		sindexCollectionType = a.ICT_LIST
	case models.MapKeySIndex:
		sindexCollectionType = a.ICT_MAPKEYS
	case models.MapValueSIndex:
		sindexCollectionType = a.ICT_MAPVALUES
	default:
		return fmt.Errorf("invalid sindex collection type: %c", si.IndexType)
	}

	var ctx []*a.CDTContext

	if si.Path.B64Context != "" {
		var err error
		ctx, err = a.Base64ToCDTContext(si.Path.B64Context)

		if err != nil {
			return fmt.Errorf("error decoding sindex context %s: %w", si.Path.B64Context, err)
		}
	}

	job, err := rw.asc.CreateComplexIndex(
		rw.writePolicy,
		si.Namespace,
		si.Set,
		si.Name,
		si.Path.BinName,
		sindexType,
		sindexCollectionType,
		ctx...,
	)
	if err != nil {
		if err.Matches(atypes.INDEX_FOUND) {
			rw.logger.Debug("index already exists, replacing it", "sindex", si.Name)

			err = rw.asc.DropIndex(rw.writePolicy, si.Namespace, si.Set, si.Name)
			if err != nil {
				return fmt.Errorf("error dropping sindex %s: %w", si.Name, err)
			}

			job, err = rw.asc.CreateComplexIndex(
				rw.writePolicy,
				si.Namespace,
				si.Set,
				si.Name,
				si.Path.BinName,
				sindexType,
				sindexCollectionType,
				ctx...,
			)
			if err != nil {
				return fmt.Errorf("error creating replacement sindex %s: %w", si.Name, err)
			}
		} else {
			return fmt.Errorf("error creating sindex %s: %w", si.Name, err)
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

	rw.logger.Debug("created sindex", "sindex", si.Name)

	return nil
}
