// Copyright 2024-2024 Aerospike, Inc.
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

package backuplib

import (
	"fmt"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-tools-backup-lib/encoding"
)

const (
	minParallel = 1
	maxParallel = 1024
)

var (
	defaultEncoderFactory = encoding.NewASBEncoderFactory()
	defaultDecoderFactory = encoding.NewASBDecoderFactory()
)

// **** Policies ****

// **** Client ****

// Config contains configuration for the backup client
type Config struct{}

// NewConfig returns a new client Config
func NewConfig() *Config {
	return &Config{}
}

// **** Backup ****

type EncoderFactory interface {
	CreateEncoder() (encoding.Encoder, error)
}

// BackupBaseConfig contains shared configuration for backup operations
type BackupBaseConfig struct {
	// EncoderFactory is used to specify the encoder with which to encode the backup data
	// if nil, the default encoder factory will be used
	EncoderFactory EncoderFactory
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used
	InfoPolicy *a.InfoPolicy
	// ScanPolicy applies to Aerospike scan operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used
	ScanPolicy *a.ScanPolicy
	// Namespace is the Aerospike namespace to backup.
	Namespace string
	// Set is the Aerospike set to backup.
	Set string
	// parallel is the number of concurrent scans to run against the Aerospike cluster.
	Parallel int
}

func (c *BackupBaseConfig) validate() error {
	if c.Parallel < minParallel || c.Parallel > maxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	return nil
}

// NewBackupBaseConfig returns a new BackupBaseConfig with default values
func NewBackupBaseConfig() *BackupBaseConfig {
	return &BackupBaseConfig{
		Parallel:       1,
		Set:            "",
		Namespace:      "test",
		EncoderFactory: defaultEncoderFactory,
	}
}

// BackupConfig contains configuration for the backup to writer operation
type BackupConfig struct {
	BackupBaseConfig
}

func (c *BackupConfig) validate() error {
	if err := c.BackupBaseConfig.validate(); err != nil {
		return err
	}

	return nil
}

// NewBackupConfig returns a new BackupToWriterConfig with default values
func NewBackupConfig() *BackupConfig {
	return &BackupConfig{
		BackupBaseConfig: *NewBackupBaseConfig(),
	}
}

// **** Restore ****

type DecoderFactory interface {
	CreateDecoder(src io.Reader) (encoding.Decoder, error)
}

// RestoreBaseConfig contains shared configuration for restore operations
type RestoreBaseConfig struct {
	// DecoderFactory is used to specify the decoder with which to decode the backup data
	// if nil, the default decoder factory will be used
	DecoderFactory DecoderFactory
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used
	InfoPolicy *a.InfoPolicy
	// WritePolicy applies to Aerospike write operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used
	WritePolicy *a.WritePolicy
	// Parallel is the number of concurrent record writers to run against the Aerospike cluster.
	Parallel int
}

func (c *RestoreBaseConfig) validate() error {
	if c.Parallel < minParallel || c.Parallel > maxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	return nil
}

// NewRestoreBaseConfig returns a new RestoreBaseConfig with default values
func NewRestoreBaseConfig() *RestoreBaseConfig {
	return &RestoreBaseConfig{
		Parallel:       4,
		DecoderFactory: defaultDecoderFactory,
	}
}

// RestoreConfig contains configuration for the restore from reader operation
type RestoreConfig struct {
	RestoreBaseConfig
}

func (c *RestoreConfig) validate() error {
	if err := c.RestoreBaseConfig.validate(); err != nil {
		return err
	}

	return nil
}

// NewRestoreConfig returns a new RestoreFromReaderConfig with default values
func NewRestoreConfig() *RestoreConfig {
	return &RestoreConfig{
		RestoreBaseConfig: *NewRestoreBaseConfig(),
	}
}
