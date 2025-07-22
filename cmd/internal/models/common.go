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

package models

import "fmt"

const DefaultChunkSize = 5 * 1024 * 1024

// Common parameters are used by both backup and restore operations.
type Common struct {
	Directory        string
	Namespace        string
	SetList          string
	BinList          string
	Parallel         int
	NoRecords        bool
	NoIndexes        bool
	NoUDFs           bool
	RecordsPerSecond int
	MaxRetries       int
	TotalTimeout     int64
	SocketTimeout    int64

	// Bandwidth is mapped to config.Bandwidth
	// Is set in MiB then converted to bytes.
	Bandwidth int64
}

func (c *Common) Validate() error {
	if c == nil {
		return nil
	}

	if c.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if c.TotalTimeout < 0 {
		return fmt.Errorf("total-timeout must be non-negative")
	}

	if c.SocketTimeout < 0 {
		return fmt.Errorf("socket-timeout must be non-negative")
	}

	if c.Parallel < 0 {
		return fmt.Errorf("parallel must be non-negative")
	}

	return nil
}
