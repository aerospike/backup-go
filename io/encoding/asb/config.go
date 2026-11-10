// Copyright 2024-2026 Aerospike, Inc.
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

package asb

import "github.com/aerospike/backup-go/models"

// EncoderConfig contains configuration options for the Encoder.
type EncoderConfig struct {
	// Namespace is the namespace to back up.
	Namespace string
	// Do not apply base-64 encoding to BLOBs: Bytes, HLL, RawMap, RawList.
	Compact bool
	// HasExpressionSIndex indicates whether the backup contains an expression SIndex.
	// In that case an asb metaVersion will be bumped.
	HasExpressionSIndex bool
	// HasSetSIndex indicates whether the backup contains a set SIndex.
	HasSetSIndex bool
	// HasVectorDataType indicates whether the database supports the vector data type.
	// In that case the records file version is bumped.
	HasVectorDataType bool
}

// NewEncoderConfig returns a new encoder EncoderConfig.
func NewEncoderConfig(
	namespace string, compact, hasVectorDataType bool, sIndexInfo models.SIndexInfo,
) *EncoderConfig {
	c := &EncoderConfig{
		Namespace:           namespace,
		Compact:             compact,
		HasExpressionSIndex: sIndexInfo.HasExpression,
		HasSetSIndex:        sIndexInfo.HasSet,
		HasVectorDataType:   hasVectorDataType,
	}

	return c
}

// getMetadataVersion resolves the ASB version of a metadata file depending on the config.
func (c *EncoderConfig) getMetadataVersion() *version {
	switch {
	case c.HasSetSIndex:
		return version33
	case c.HasExpressionSIndex:
		return version32
	default:
		return version31
	}
}

// getRecordsVersion resolves the ASB version of a records file depending on the config.
func (c *EncoderConfig) getRecordsVersion() *version {
	switch {
	case c.HasVectorDataType:
		return version34
	default:
		return version31
	}
}
