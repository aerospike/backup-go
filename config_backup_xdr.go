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
	"regexp"
	"strconv"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

var expDCName = regexp.MustCompile(`^[a-zA-Z0-9_\-$]+$`)

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
	// For XDR must be set to `EncoderTypeASBX` = 1.
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
	// MaxThroughput number for xdr.
	MaxThroughput int
	// TLS config for secure XDR connection.
	TLSConfig *tls.Config
	// Timeout for TCP read operations.
	// Used by TCP server for XDR.
	ReadTimeout time.Duration
	// Timeout for TCP writes operations.
	// Used by TCP server for XDR.
	WriteTimeout time.Duration
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
	InfoPolingPeriod time.Duration
	// Timeout for starting TCP server for XDR.
	// If the TCP server for XDR does not receive any data within this timeout period, it will shut down.
	// This situation can occur if the LocalAddress and LocalPort options are misconfigured.
	StartTimeout time.Duration
	// Retry policy for info commands.
	InfoRetryPolicy *models.RetryPolicy
}

// validate validates the ConfigBackupXDR.
//
//nolint:gocyclo // contains a long list of validations
func (c *ConfigBackupXDR) validate() error {
	if err := validateRewind(c.Rewind); err != nil {
		return err
	}

	if c.FileLimit < 0 {
		return fmt.Errorf("filelimit value must not be negative, got %d", c.FileLimit)
	}

	if c.DC == "" {
		return fmt.Errorf("dc name must not be empty")
	}

	if len(c.DC) > 31 {
		return fmt.Errorf("dc name must be less than 32 characters")
	}

	if !expDCName.MatchString(c.DC) {
		return fmt.Errorf("dc name must match ^[a-zA-Z0-9_\\-$]+$")
	}

	if c.ParallelWrite < MinParallel || c.ParallelWrite > MaxParallel {
		return fmt.Errorf("parallel write must be between 1 and 1024, got %d", c.ParallelWrite)
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

	if c.ReadTimeout < 0 {
		return fmt.Errorf("read timeout must not be negative, got %d", c.ReadTimeout)
	}

	if c.WriteTimeout < 0 {
		return fmt.Errorf("write timeout must not be negative, got %d", c.WriteTimeout)
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

	if c.InfoPolingPeriod < 1 {
		return fmt.Errorf("info poling period must not be less than 1, got %d", c.InfoPolingPeriod)
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

	if err := c.InfoRetryPolicy.Validate(); err != nil {
		return fmt.Errorf("invalid info retry policy: %w", err)
	}

	if c.EncoderType != EncoderTypeASBX {
		return fmt.Errorf("unsuported encoder type: %d", c.EncoderType)
	}

	if c.MaxThroughput%100 != 0 {
		return fmt.Errorf("max throughput must be a multiple of 100, got %d", c.MaxThroughput)
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
