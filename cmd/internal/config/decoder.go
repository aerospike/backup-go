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

	"gopkg.in/yaml.v3"
)

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
