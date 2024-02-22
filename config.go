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

	"github.com/aerospike/aerospike-tools-backup-lib/models"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	minParallel = 1
	maxParallel = 1024
)

var (
	defaultEncoderBuilder = NewASBEncoderBuilder()
	defaultDecoderBuilder = NewASBDecoderBuilder()
)

// **** Policies ****

// Policies contains the Aerospike policies used during backup and restore operations
type Policies struct {
	// InfoPolicy applies to Aerospike Info requests made
	// If nil, the Aerospike client's default policy will be used
	InfoPolicy *a.InfoPolicy
	// WritePolicy applies to Aerospike write operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used
	WritePolicy *a.WritePolicy
	// ScanPolicy applies to Aerospike scan operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used
	ScanPolicy *a.ScanPolicy
}

// **** Client ****

// Config contains configuration for the backup client
type Config struct {
	// Policies contains the Aerospike policies used during backup and restore operations
	// If nil, the Aerospike client's default policies will be used
	// These policies will be used as defaults for the backup and restore operations
	Policies *Policies
}

// NewConfig returns a new client Config
func NewConfig() *Config {
	return &Config{}
}

// **** Backup ****

type Encoder interface {
	EncodeRecord(*models.Record) ([]byte, error)
	EncodeUDF(*models.UDF) ([]byte, error)
	EncodeSIndex(*models.SIndex) ([]byte, error)
}

type EncoderBuilder interface {
	CreateEncoder() (Encoder, error)
}

// BackupBaseConfig contains shared configuration for backup operations
type BackupBaseConfig struct {
	// Parallel is the number of parallel scans to run against the Aerospike cluster
	// during a backup operation
	Parallel int
	// Namespace is the Aerospike namespace to backup
	Namespace string
	// Set is the Aerospike set to backup
	Set string
	// EncoderBuilder is used to specify the encoder with which to encode the backup data
	// If nil, the default encoder will be used
	EncoderBuilder EncoderBuilder
	// Policies contains the Aerospike policies used during backup operations
	// These policies override the default policies from the backup client's configuration
	// If nil, the backup client's policies will be used
	Policies *Policies
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
		EncoderBuilder: defaultEncoderBuilder,
	}
}

// BackupToWriterConfig contains configuration for the backup to writer operation
type BackupToWriterConfig struct {
	BackupBaseConfig
}

func (c *BackupToWriterConfig) validate() error {
	if err := c.BackupBaseConfig.validate(); err != nil {
		return err
	}
	return nil
}

// NewBackupToWriterConfig returns a new BackupToWriterConfig with default values
func NewBackupToWriterConfig() *BackupToWriterConfig {
	return &BackupToWriterConfig{
		BackupBaseConfig: *NewBackupBaseConfig(),
	}
}

// **** Restore ****

type Decoder interface {
	NextToken() (any, error)
}

type DecoderBuilder interface {
	CreateDecoder() (Decoder, error)
	SetSource(src io.Reader)
}

// RestoreBaseConfig contains shared configuration for restore operations
type RestoreBaseConfig struct {
	// Parallel is the number of parallel writers to run against the Aerospike cluster
	// during a restore operation
	Parallel int
	// DecoderBuilder is used to specify the decoder with which to decode backup data during restores
	// If nil, the default decoder will be used
	DecoderBuilder DecoderBuilder
	// Policies contains the Aerospike policies used during restore operations
	// These policies override the default policies from the backup client's configuration
	// If nil, the backup client's policies will be used
	Policies *Policies
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
		DecoderBuilder: defaultDecoderBuilder,
	}
}

// RestoreFromReaderConfig contains configuration for the restore from reader operation
type RestoreFromReaderConfig struct {
	RestoreBaseConfig
}

func (c *RestoreFromReaderConfig) validate() error {
	if err := c.RestoreBaseConfig.validate(); err != nil {
		return err
	}
	return nil
}

// NewRestoreFromReaderConfig returns a new RestoreFromReaderConfig with default values
func NewRestoreFromReaderConfig() *RestoreFromReaderConfig {
	return &RestoreFromReaderConfig{
		RestoreBaseConfig: *NewRestoreBaseConfig(),
	}
}
