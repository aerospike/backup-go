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

package config

import (
	"fmt"
	"os"

	"github.com/aerospike/backup-go/cmd/internal/config/dto"
	"gopkg.in/yaml.v3"
)

// decodeBackupServiceConfig reads a backup configuration file and decodes it into BackupServiceConfig.
// Returns an error on failure.
func decodeBackupServiceConfig(filename string) (*BackupServiceConfig, error) {
	var backupDto dto.Backup
	if err := decodeFromFile(filename, &backupDto); err != nil {
		return nil, err
	}

	serviceConfig, err := dtoToBackupServiceConfig(&backupDto)
	if err != nil {
		return nil, err
	}

	return serviceConfig, nil
}

func dtoToBackupServiceConfig(dtoBackup *dto.Backup) (*BackupServiceConfig, error) {
	asConfig, err := dtoBackup.Cluster.ToAerospikeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map to aerospike config: %w", err)
	}

	return &BackupServiceConfig{
		App:          dtoBackup.App.ToModelApp(),
		ClientConfig: asConfig,
		ClientPolicy: dtoBackup.Cluster.ToModelClientPolicy(),
		Backup:       dtoBackup.ToModelBackup(),
		Compression:  dtoBackup.Compression.ToModelCompression(),
		Encryption:   dtoBackup.Encryption.ToModelEncryption(),
		SecretAgent:  dtoBackup.SecretAgent.ToModelSecretAgent(),
		AwsS3:        dtoBackup.Aws.S3.ToModelAwsS3(),
		GcpStorage:   dtoBackup.Gcp.Storage.ToModelGcpStorage(),
		AzureBlob:    dtoBackup.Azure.Blob.ToModelAzureBlob(),
	}, nil
}

// decodeRestoreServiceConfig reads a restore configuration file and decodes it into RestoreServiceConfig.
// Returns an error on failure.
func decodeRestoreServiceConfig(filename string) (*RestoreServiceConfig, error) {
	var restoreDto dto.Restore
	if err := decodeFromFile(filename, &restoreDto); err != nil {
		return nil, err
	}

	serviceConfig, err := dtoToRestoreServiceConfig(&restoreDto)
	if err != nil {
		return nil, err
	}

	return serviceConfig, nil
}

func dtoToRestoreServiceConfig(dtoRestore *dto.Restore) (*RestoreServiceConfig, error) {
	asConfig, err := dtoRestore.Cluster.ToAerospikeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map to aerospike config: %w", err)
	}

	return &RestoreServiceConfig{
		App:          dtoRestore.App.ToModelApp(),
		ClientConfig: asConfig,
		ClientPolicy: dtoRestore.Cluster.ToModelClientPolicy(),
		Restore:      dtoRestore.ToModelRestore(),
		Compression:  dtoRestore.Compression.ToModelCompression(),
		Encryption:   dtoRestore.Encryption.ToModelEncryption(),
		SecretAgent:  dtoRestore.SecretAgent.ToModelSecretAgent(),
		AwsS3:        dtoRestore.Aws.S3.ToModelAwsS3(),
		GcpStorage:   dtoRestore.Gcp.Storage.ToModelGcpStorage(),
		AzureBlob:    dtoRestore.Azure.Blob.ToModelAzureBlob(),
	}, nil
}

// decodeFromFile decode yaml to params.
func decodeFromFile(filename string, params any) error {
	if filename == "" {
		return fmt.Errorf("config path is empty")
	}

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open config file %s: %w", filename, err)
	}
	defer file.Close()

	yamlDec := yaml.NewDecoder(file)
	yamlDec.KnownFields(true)

	if err := yamlDec.Decode(params); err != nil {
		return fmt.Errorf("faield to decode config file %s: %w", filename, err)
	}

	return nil
}

// DumpFile used for tests.
func DumpFile(params any) error {
	filename := "dump.yaml"

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("failed to open config file %s: %w", filename, err)
	}
	defer file.Close()

	yamlEnc := yaml.NewEncoder(file)
	if err := yamlEnc.Encode(params); err != nil {
		return fmt.Errorf("failed to encode config file %s: %w", filename, err)
	}

	return nil
}
