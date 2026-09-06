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

package asb

import (
	"fmt"
	"math"

	"github.com/aerospike/backup-go/errclass"
)

// readSignedInt reads a signed int64 directly from the reader without string allocation.
func readSignedInt(src *countingReader, delim byte) (int64, error) {
	var result uint64

	negative := false
	hasDigits := false

	for {
		b, err := src.ReadByte()
		if err != nil {
			return 0, err
		}

		if b == delim {
			if err := src.UnreadByte(); err != nil {
				return 0, err
			}

			break
		}

		// Handle negative sign - must be first character
		if b == '-' {
			if hasDigits || negative {
				return 0, fmt.Errorf("%w: invalid number: unexpected '-'", errclass.ErrCorruptData)
			}

			negative = true

			continue
		}

		if b < '0' || b > '9' {
			return 0, fmt.Errorf("%w: invalid number character: %c", errclass.ErrCorruptData, b)
		}

		hasDigits = true
		result = result*10 + uint64(b-'0')
	}

	if !hasDigits {
		return 0, fmt.Errorf("%w: empty number", errclass.ErrCorruptData)
	}

	if negative {
		return -int64(result), nil
	}

	return int64(result), nil
}

// readUnsignedInt reads an unsigned uint32 directly from the reader without string allocation.
func readUnsignedInt(src *countingReader, delim byte) (uint32, error) {
	var result uint64

	hasDigits := false

	for {
		b, err := src.ReadByte()
		if err != nil {
			return 0, err
		}

		if b == delim {
			if err := src.UnreadByte(); err != nil {
				return 0, err
			}

			break
		}

		if b < '0' || b > '9' {
			return 0, fmt.Errorf("%w: invalid number character: %c", errclass.ErrCorruptData, b)
		}

		hasDigits = true
		result = result*10 + uint64(b-'0')

		// Check for uint32 overflow
		if result > math.MaxUint32 {
			// Consume remaining digits
			if err := consumeUntil(src, delim); err != nil {
				return math.MaxUint32, fmt.Errorf("%w: value exceeds uint32 max", errclass.ErrCorruptData)
			}

			return math.MaxUint32, fmt.Errorf("%w: value exceeds uint32 max", errclass.ErrCorruptData)
		}
	}

	if !hasDigits {
		return 0, fmt.Errorf("%w: empty number", errclass.ErrCorruptData)
	}

	return uint32(result), nil
}

// consumeUntil reads and discards bytes until the delimiter is found.
func consumeUntil(src *countingReader, delim byte) error {
	for {
		b, err := src.ReadByte()
		if err != nil {
			return err
		}

		if b == delim {
			return src.UnreadByte()
		}
	}
}
