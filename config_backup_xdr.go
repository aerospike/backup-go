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
	"crypto/tls"
	"fmt"
	"strconv"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// ConfigBackupXDR contains configuration for the xdr backup operation.
type ConfigBackupXDR struct {
	// InfoPolicy applies to Aerospike Info requests made during backup and
	// restore. If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// Encryption details.
	EncryptionPolicy *EncryptionPolicy
	// Compression details.
	CompressionPolicy *CompressionPolicy
	// Secret agent config.
	SecretAgentConfig *SecretAgentConfig
	// EncoderType describes an Encoder type that will be used on backing up.
	// Default `EncoderTypeASBX` = 1.
	EncoderType EncoderType
	// File size limit (in bytes) for the backup. If a backup file exceeds this
	// size threshold, a new file will be created. 0 for no file size limit.
	FileLimit int64
	// ParallelWrite is the number of concurrent backup files writing.
	ParallelWrite int
	// DC name of dc that will be created on source instance.
	DC string
	// Local address, where source cluster will send data.
	LocalAddress string
	// Local port, where source cluster will send data.
	LocalPort int
	// Namespace is the Aerospike namespace to back up.
	Namespace string
	// Rewind is used to ship all existing records of a namespace.
	// When rewinding a namespace, XDR will scan through the index and ship
	// all the records for that namespace, partition by partition.
	// Can be `all` or number of seconds.
	Rewind string
	// TLS config for secure XDR connection.
	TLSConfig *tls.Config
	// Timeout in milliseconds for TCP read operations.
	// Used by TCP server for XDR.
	ReadTimoutMilliseconds int64
	// Timeout in milliseconds for TCP writes operations.
	// Used by TCP server for XDR.
	WriteTimeoutMilliseconds int64
	// Results queue size.
	// Used by TCP server for XDR.
	ResultQueueSize int
	// Ack messages queue size.
	// Used by TCP server for XDR.
	AckQueueSize int
	// Max number of allowed simultaneous connection to server.
	// Used by TCP server for XDR.
	MaxConnections int
	// How often a backup client will send info commands to check aerospike cluster stats.
	// To measure recovery state and lag.
	InfoPolingPeriodMilliseconds int64
}

func (c *ConfigBackupXDR) validate() error {
	if err := validateRewind(c.Rewind); err != nil {
		return err
	}

	if c.FileLimit < 0 {
		return fmt.Errorf("filelimit value must not be negative, got %d", c.FileLimit)
	}

	if c.ParallelWrite < MinParallel || c.ParallelWrite > MaxParallel {
		return fmt.Errorf("parallel write must be between 1 and 1024, got %d", c.ParallelWrite)
	}

	if c.DC == "" {
		return fmt.Errorf("dc name must not be empty")
	}

	if c.LocalAddress == "" {
		return fmt.Errorf("local address must not be empty")
	}

	if c.LocalPort < 0 || c.LocalPort > 65535 {
		return fmt.Errorf("local port must be between 0 and 65535, got %d", c.LocalPort)
	}

	if c.Namespace == "" {
		return fmt.Errorf("namespace must not be empty")
	}

	if c.ReadTimoutMilliseconds < 0 {
		return fmt.Errorf("read timout must not be negative, got %d", c.ReadTimoutMilliseconds)
	}

	if c.WriteTimeoutMilliseconds < 0 {
		return fmt.Errorf("write timeout must not be negative, got %d", c.WriteTimeoutMilliseconds)
	}

	if c.ResultQueueSize < 0 {
		return fmt.Errorf("result queue size must not be negative, got %d", c.ResultQueueSize)
	}

	if c.AckQueueSize < 0 {
		return fmt.Errorf("ack queue size must not be negative, got %d", c.AckQueueSize)
	}

	if c.MaxConnections < 1 {
		return fmt.Errorf("max connections must not be less than 1, got %d", c.MaxConnections)
	}

	if c.InfoPolingPeriodMilliseconds < 0 {
		return fmt.Errorf("info poling period must not be negative, got %d", c.InfoPolingPeriodMilliseconds)
	}

	if err := c.CompressionPolicy.validate(); err != nil {
		return fmt.Errorf("compression policy invalid: %w", err)
	}

	if err := c.EncryptionPolicy.validate(); err != nil {
		return fmt.Errorf("encryption policy invalid: %w", err)
	}

	if err := c.SecretAgentConfig.validate(); err != nil {
		return fmt.Errorf("secret agent invalid: %w", err)
	}

	return nil
}

func validateRewind(value string) error {
	if value == "all" {
		return nil
	}

	num, err := strconv.ParseUint(value, 10, 64)
	if err != nil || num == 0 {
		return fmt.Errorf("rewind must be a positive number or 'all', got: %s", value)
	}

	return nil
}
