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

package streamers

import (
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
)

const (
	// manifestNameSeparator separates the fields of a manifest name.
	manifestNameSeparator = '-'
	// scanSpace is the white space a scanf conversion skips, as isspace has it.
	scanSpace = " \t\n\v\f\r"
	// decimalBase and hexBase are the bases of the numbers a name holds.
	decimalBase = 10
	hexBase     = 16
)

// partitionOfManifest returns the partition a query stream manifest describes,
// read from its file name, {pid}-{regime}-{timestamp}-{uuid}.json, the way the
// server reads it when it groups manifests for a restore:
// sscanf(leaf, "%u-%u-%llu-%x.json") must convert all four numbers, and the
// partition must exist. ok is false for any other name, which the server skips
// on restore.
func partitionOfManifest(manifestPath string) (id int, ok bool) {
	sc := nameScanner{rest: path.Base(manifestPath)}

	pid, ok := sc.number(decimalBase)
	if !ok || !sc.literal(manifestNameSeparator) {
		return 0, false
	}

	// regime, timestamp and uuid are not needed here, but a name the server
	// cannot convert them from is a manifest it never reads.
	if _, ok := sc.number(decimalBase); !ok || !sc.literal(manifestNameSeparator) {
		return 0, false
	}

	if _, ok := sc.number(decimalBase); !ok || !sc.literal(manifestNameSeparator) {
		return 0, false
	}

	// The ".json" the format ends with is no conversion, so sscanf counts four
	// whether or not it matches.
	if _, ok := sc.number(hexBase); !ok {
		return 0, false
	}

	// %u stores into an unsigned int.
	if p := uint32(pid); p < segment.PartitionCount {
		return int(p), true
	}

	return 0, false
}

// nameScanner reads a file name the way scanf reads its input.
type nameScanner struct {
	rest string
}

// literal consumes c, which an ordinary character of a scanf format must
// match exactly.
func (sc *nameScanner) literal(c byte) bool {
	if sc.rest == "" || sc.rest[0] != c {
		return false
	}

	sc.rest = sc.rest[1:]

	return true
}

// number consumes an unsigned scanf conversion in base 10 or 16: optional
// white space, an optional sign, in base 16 an optional 0x prefix, then at
// least one digit. Like the strtoul behind it, a negative number wraps around
// and one too large saturates.
func (sc *nameScanner) number(base int) (uint64, bool) {
	s := strings.TrimLeft(sc.rest, scanSpace)

	negative := false
	if s != "" && (s[0] == '+' || s[0] == '-') {
		negative = s[0] == '-'
		s = s[1:]
	}

	if base == hexBase && len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') &&
		isDigit(s[2], base) {
		s = s[2:]
	}

	end := 0
	for end < len(s) && isDigit(s[end], base) {
		end++
	}

	if end == 0 {
		return 0, false
	}

	sc.rest = s[end:]

	n, err := strconv.ParseUint(s[:end], base, 64)

	switch {
	case err != nil:
		// The digits were checked, so only the range can be exceeded.
		return math.MaxUint64, true
	case negative:
		return -n, true
	default:
		return n, true
	}
}

// isDigit reports whether c is a digit of base 10 or 16.
func isDigit(c byte, base int) bool {
	switch {
	case c >= '0' && c <= '9':
		return true
	case base == hexBase:
		return (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
	default:
		return false
	}
}
