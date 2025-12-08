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
	"fmt"
)

// Compression modes
const (
	// CompressNone no compression.
	CompressNone = "NONE"
	// CompressZSTD compression using ZSTD.
	CompressZSTD = "ZSTD"
)

// CompressionPolicy contains backup compression information.
type CompressionPolicy struct {
	// The compression mode to be used (default is NONE).
	Mode string
	// The compression level to use (or -1 if unspecified).
	Level int
}

// NewCompressionPolicy returns a new compression policy for backup/restore operations.
func NewCompressionPolicy(mode string, level int) *CompressionPolicy {
	return &CompressionPolicy{
		Mode:  mode,
		Level: level,
	}
}

// validate validates the compression policy parameters.
func (p *CompressionPolicy) validate() error {
	if p == nil {
		return nil
	}

	if p.Mode != CompressNone && p.Mode != CompressZSTD {
		return fmt.Errorf("invalid compression mode: %s", p.Mode)
	}

	if p.Level == 0 {
		p.Level = -1
	}

	if p.Level < -1 {
		return fmt.Errorf("invalid compression level: %d", p.Level)
	}

	return nil
}
