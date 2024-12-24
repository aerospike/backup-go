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

package asbx

import (
	"fmt"
	"path/filepath"
)

// Validator represents backup files validator.
type Validator struct {
}

// NewValidator returns new validator instance for files validation.
func NewValidator() *Validator {
	return &Validator{}
}

// Run performs backup files validation.
func (v *Validator) Run(fileName string) error {
	if filepath.Ext(fileName) != ".asbx" {
		return fmt.Errorf("restore file %s is in an invalid format, expected extension: .asbx, got: %s",
			fileName, filepath.Ext(fileName))
	}

	return nil
}
