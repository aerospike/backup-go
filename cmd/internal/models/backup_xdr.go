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

import (
	"fmt"
)

// BackupXDR flags that will be mapped to xdr backup config.
type BackupXDR struct {
	Directory                    string
	FileLimit                    uint64
	RemoveFiles                  bool
	ParallelWrite                int
	DC                           string
	LocalAddress                 string
	LocalPort                    int
	Namespace                    string
	Rewind                       string
	MaxThroughput                int
	ReadTimeoutMilliseconds      int64
	WriteTimeoutMilliseconds     int64
	ResultQueueSize              int
	AckQueueSize                 int
	MaxConnections               int
	InfoPolingPeriodMilliseconds int64
	StartTimeoutMilliseconds     int64

	StopXDR    bool `yaml:"stop-xdr,omitempty"`
	UnblockMRT bool `yaml:"unblock-mrt,omitempty"`

	InfoMaxRetries                uint
	InfoRetriesMultiplier         float64
	InfoRetryIntervalMilliseconds int64

	Forward bool
}

func (b *BackupXDR) Validate() error {
	if b == nil {
		return nil
	}

	if b.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if b.DC == "" {
		return fmt.Errorf("dc is required")
	}

	if b.LocalAddress == "" {
		return fmt.Errorf("local address is required")
	}

	if b.ReadTimeoutMilliseconds < 0 {
		return fmt.Errorf("backup xdr read timeout can't be negative")
	}

	if b.WriteTimeoutMilliseconds < 0 {
		return fmt.Errorf("backup xdr write timeout can't be negative")
	}

	if b.InfoPolingPeriodMilliseconds < 0 {
		return fmt.Errorf("backup xdr info poling period can't be negative")
	}

	if b.StartTimeoutMilliseconds < 0 {
		return fmt.Errorf("backup xdr start timeout can't be negative")
	}

	if b.ResultQueueSize < 0 {
		return fmt.Errorf("backup xdr result queue size can't be negative")
	}

	if b.AckQueueSize < 0 {
		return fmt.Errorf("backup xdr ack queue size can't be negative")
	}

	if b.MaxConnections < 1 {
		return fmt.Errorf("backup xdr max connections can't be less than 1")
	}

	if b.ParallelWrite < 0 {
		return fmt.Errorf("backup xdr parallel write can't be negative")
	}

	if b.FileLimit < 1 {
		return fmt.Errorf("backup xdr file limit can't be less than 1")
	}

	if b.InfoRetryIntervalMilliseconds < 0 {
		return fmt.Errorf("backup xdr info retry interval can't be negative")
	}

	if b.InfoRetriesMultiplier < 0 {
		return fmt.Errorf("backup xdr info retries multiplier can't be negative")
	}

	return nil
}
