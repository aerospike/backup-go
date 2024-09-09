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

package flags

import (
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type Encryption struct {
	models.Encryption
}

func NewEncryption() *Encryption {
	return &Encryption{}
}

func (f *Encryption) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Mode, "encrypt",
		"",
		"Enables encryption of backup files using the specified encryption algorithm.\n"+
			"Supported encryption algorithms are: NONE, AES128, AES256.\n"+
			"A private key must be given, either via the --encryption-key-file option or\n"+
			"the --encryption-key-env option or the --encryption-key-secret.")
	flagSet.StringVar(&f.KeyFile, "encryption-key-file",
		"",
		"Grabs the encryption key from the given file, which must be in PEM format.")
	flagSet.StringVar(&f.KeyEnv, "encryption-key-env",
		"",
		"Grabs the encryption key from the given environment variable, which must be base-64 encoded.")
	flagSet.StringVar(&f.KeySecret, "encryption-key-secret",
		"",
		"Grabs the encryption key from secret-agent.")

	return flagSet
}

func (f *Encryption) GetEncryption() *models.Encryption {
	return &f.Encryption
}
