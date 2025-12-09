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
	"errors"
	"fmt"
)

// Encryption modes
const (
	// EncryptNone no encryption.
	EncryptNone = "NONE"
	// EncryptAES128 encryption using AES128 algorithm.
	EncryptAES128 = "AES128"
	// EncryptAES256 encryption using AES256 algorithm.
	EncryptAES256 = "AES256"
)

// EncryptionPolicy contains backup encryption information.
type EncryptionPolicy struct {
	// The path to the file containing the encryption key.
	KeyFile *string
	// The name of the environment variable containing the encryption key.
	KeyEnv *string
	// The secret keyword in Aerospike Secret Agent containing the encryption key.
	KeySecret *string
	// The encryption mode to be used (NONE, AES128, AES256)
	Mode string
}

// validate validates the encryption policy.
func (p *EncryptionPolicy) validate() error {
	if p == nil {
		return nil
	}

	if p.Mode != EncryptNone && p.Mode != EncryptAES128 && p.Mode != EncryptAES256 {
		return fmt.Errorf("invalid encryption mode: %s", p.Mode)
	}

	if p.Mode != EncryptNone && p.KeyFile == nil && p.KeyEnv == nil && p.KeySecret == nil {
		return errors.New("encryption key location not specified")
	}

	// Only one parameter allowed to be set.
	if (p.KeyFile != nil && p.KeyEnv != nil) ||
		(p.KeyFile != nil && p.KeySecret != nil) ||
		(p.KeyEnv != nil && p.KeySecret != nil) {
		return fmt.Errorf("only one encryption key source may be specified")
	}

	return nil
}
