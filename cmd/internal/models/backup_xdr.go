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

import "crypto/tls"

// BackupXDR flags that will be mapped to xdr backup config.
// (common for backup and restore flags are in Common).
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

	TLSConfig *tls.Config

	StopXDR    bool
	UnblockMRT bool

	InfoMaxRetries                uint
	InfoRetriesMultiplier         float64
	InfoRetryIntervalMilliseconds int64

	Forward bool
}
