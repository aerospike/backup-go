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

package asb

// EncoderConfig contains configuration options for the Encoder.
type EncoderConfig struct {
	// Namespace is the namespace to back up.
	Namespace string
	// Do not apply base-64 encoding to BLOBs: Bytes, HLL, RawMap, RawList.
	Compact bool
	// HasExpressionSIndex indicates whether the backup contains an expression SIndex.
	// In that case an asb metaVersion will be bumped.
	HasExpressionSIndex bool
}

// NewEncoderConfig returns a new encoder EncoderConfig.
func NewEncoderConfig(namespace string, compact, hasExpressionSIndex bool) *EncoderConfig {
	return &EncoderConfig{
		Namespace:           namespace,
		Compact:             compact,
		HasExpressionSIndex: hasExpressionSIndex,
	}
}

// getVersion resolves version depending on the config.
func (c *EncoderConfig) getVersion() *version {
	if c.HasExpressionSIndex {
		return version32
	}

	return version31
}
