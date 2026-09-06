// Copyright 2024-2026 Aerospike, Inc.
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

package local

import (
	"errors"
	"os"

	"github.com/aerospike/backup-go/errclass"
)

// classifyFS returns the error class of a file system failure. A path that does
// not exist is [errclass.ErrNotFound]; everything else is a storage failure.
// Every place where an os error enters this package goes through it, so the
// same syscall error is never reported under two different classes.
func classifyFS(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return errclass.ErrNotFound
	}

	return errclass.ErrStorage
}
