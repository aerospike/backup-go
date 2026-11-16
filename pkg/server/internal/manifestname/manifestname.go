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

// Package manifestname reads the names of the manifests of a server-integrated
// backup the way the server reads them on restore, which decides which
// manifests a restore applies.
package manifestname

import (
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
)

const (
	// separator separates the fields of a manifest name.
	separator = '-'
	// suffix ends the name of every manifest.
	suffix = ".json"
	// scanSpace is the white space a scanf conversion skips, as isspace has it.
	scanSpace = " \t\n\v\f\r"
	// decimalBase and hexBase are the bases of the numbers a name holds.
	decimalBase = 10
	hexBase     = 16
)

// Name is what the server reads from a manifest name.
type Name struct {
	// Partition is the partition a query stream manifest describes; zero for
	// a change stream manifest.
	Partition int
	// Regime is the partition reservation regime the manifest was written
	// under. It only grows, across owners and restarts.
	Regime uint32
	// Timestamp orders the manifests written under one regime.
	Timestamp uint64
}

// Newer reports whether n supersedes other: a higher regime, or the same
// regime and a later timestamp, as the server ranks manifests.
func (n Name) Newer(other Name) bool {
	return n.Regime > other.Regime || (n.Regime == other.Regime && n.Timestamp > other.Timestamp)
}

// ParseQuery reads the name of the query stream manifest at manifestPath,
// {pid}-{regime}-{timestamp}-{uuid}.json, the way the server reads it when it
// groups manifests for a restore: sscanf(leaf, "%u-%u-%llu-%x.json") must
// convert all four numbers, and the partition must exist. ok is false for any
// other name, which the server skips on restore.
func ParseQuery(manifestPath string) (name Name, ok bool) {
	sc := scanner{rest: path.Base(manifestPath)}

	pid, ok := sc.number(decimalBase)
	if !ok || !sc.separator() {
		return Name{}, false
	}

	regime, ok := sc.number(decimalBase)
	if !ok || !sc.separator() {
		return Name{}, false
	}

	timestamp, ok := sc.number(decimalBase)
	if !ok || !sc.separator() {
		return Name{}, false
	}

	// The uuid is not needed, but a name the server cannot convert it from is
	// a manifest it never reads. The ".json" the format ends with is no
	// conversion, so sscanf counts four whether or not it matches.
	if _, ok := sc.number(hexBase); !ok {
		return Name{}, false
	}

	// %u stores into an unsigned int.
	p := uint32(pid)
	if p >= segment.PartitionCount {
		return Name{}, false
	}

	return Name{Partition: int(p), Regime: uint32(regime), Timestamp: timestamp}, true
}

// ParseChange reads the name of a change stream manifest,
// {regime}-{timestamp}-{uuid}.json, the way the server reads it when it picks
// the manifest of a node to restore. leaf is what follows /manifest/ in the
// key of the manifest, whole: sscanf(leaf, "%u-%llu-") must convert both
// numbers, and leaf must end in .json. ok is false for any other leaf, which
// the server skips on restore.
func ParseChange(leaf string) (name Name, ok bool) {
	if !strings.HasSuffix(leaf, suffix) {
		return Name{}, false
	}

	sc := scanner{rest: leaf}

	regime, ok := sc.number(decimalBase)
	if !ok || !sc.separator() {
		return Name{}, false
	}

	// The '-' after the timestamp is no conversion, so sscanf counts two
	// whether or not it matches.
	timestamp, ok := sc.number(decimalBase)
	if !ok {
		return Name{}, false
	}

	// %u stores into an unsigned int.
	return Name{Regime: uint32(regime), Timestamp: timestamp}, true
}

// scanner reads a file name the way scanf reads its input.
type scanner struct {
	rest string
}

// separator consumes the separator, an ordinary character of the scanf
// format, which the input must match exactly.
func (sc *scanner) separator() bool {
	if sc.rest == "" || sc.rest[0] != separator {
		return false
	}

	sc.rest = sc.rest[1:]

	return true
}

// number consumes an unsigned scanf conversion in base 10 or 16: optional
// white space, an optional sign, in base 16 an optional 0x prefix, then at
// least one digit. Like the strtoul behind it, a negative number wraps around
// and one too large saturates.
func (sc *scanner) number(base int) (uint64, bool) {
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
