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

package compression

import (
	"errors"

	"github.com/klauspost/compress/zstd"
)

// IsCorruptedError checks if the error belongs to the list of corrupted errors from zstd lib.
func IsCorruptedError(err error) bool {
	return errors.Is(err, zstd.ErrReservedBlockType) ||
		errors.Is(err, zstd.ErrCompressedSizeTooBig) ||
		errors.Is(err, zstd.ErrWindowSizeExceeded) ||
		errors.Is(err, zstd.ErrWindowSizeTooSmall) ||
		errors.Is(err, zstd.ErrMagicMismatch)
}
