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

package asinfo

import (
	"crypto/rand"
	"encoding/binary"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// jobIDTimeLayout formats the timestamp part of a job id in UTC, e.g. 260316T142035.
	jobIDTimeLayout = "060102T150405"
	// jobIDSeparator divides the timestamp from the salt.
	jobIDSeparator = '-'
	// jobIDSaltLen is the width of the random part, in base36 digits.
	jobIDSaltLen = 4
	// jobIDSaltSpace is 36^jobIDSaltLen: the number of distinct salt values.
	jobIDSaltSpace uint32 = 36 * 36 * 36 * 36
	// jobIDLen is the full width of a generated id: timestamp, separator and salt.
	jobIDLen = len(jobIDTimeLayout) + 1 + jobIDSaltLen
	// jobIDMaxLen is the server's cap on a job id. JOB_ID_MAX_SIZE is 20 in backup.h;
	// one byte is reserved for a possible NUL terminator.
	jobIDMaxLen = 19

	base36Digits = "0123456789abcdefghijklmnopqrstuvwxyz"

	// charClassMeta lists the characters that carry meaning inside a regular
	// expression character class: the closing bracket, the negation mark, the range
	// mark, the escape itself and the bracket that opens a POSIX class such as
	// [:digit:].
	charClassMeta = `]^-\[`
)

// jobIDRegexp matches the shape of an id built by newJobIDForTime. It is compiled
// once at start up, because IsJobID runs once per listed storage prefix.
var jobIDRegexp = regexp.MustCompile(jobIDPattern())

// jobIDPattern builds the regular expression for a job id out of the same constants
// the generator uses, so that changing the format cannot leave the matcher behind.
// Every digit of jobIDTimeLayout stands for one decimal digit and every other
// character of the layout matches literally, followed by jobIDSeparator and a salt of
// jobIDSaltLen base36 digits. Only the form is constrained: the timestamp fields are
// not checked to be a real date.
func jobIDPattern() string {
	var b strings.Builder

	b.WriteByte('^')

	for i := range len(jobIDTimeLayout) {
		if c := jobIDTimeLayout[i]; c >= '0' && c <= '9' {
			b.WriteString(`\d`)
		} else {
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}

	b.WriteString(regexp.QuoteMeta(string(jobIDSeparator)))
	b.WriteByte('[')
	b.WriteString(quoteCharClass(base36Digits))
	b.WriteString("]{")
	b.WriteString(strconv.Itoa(jobIDSaltLen))
	b.WriteString("}$")

	return b.String()
}

// quoteCharClass escapes s so that each of its characters stands for itself inside a
// regular expression character class. regexp.QuoteMeta cannot be used here: it leaves
// '-' and '^' alone, because they are literal outside a class, so a salt alphabet that
// gained a '-' would silently turn into a range instead of failing to compile.
func quoteCharClass(s string) string {
	var b strings.Builder

	b.Grow(len(s))

	for i := range len(s) {
		c := s[i]
		if strings.ContainsRune(charClassMeta, rune(c)) {
			b.WriteByte('\\')
		}

		b.WriteByte(c)
	}

	return b.String()
}

// Compile-time guard: an id that exceeds the server cap must not reach the cluster.
// An array cannot have a negative length, so this declaration fails to build if
// jobIDLen ever grows past jobIDMaxLen.
var _ [jobIDMaxLen - jobIDLen]struct{}

// jobIDSaltCounter hands out salt values. It starts at a random point so that two
// processes backing up the same cluster are unlikely to walk the same sequence, and
// then increments, so a single process walks jobIDSaltSpace distinct salts before it
// repeats one. The only exception is the counter overflowing uint32, which shifts the
// sequence and is 4 billion ids away.
var jobIDSaltCounter atomic.Uint32

func init() {
	var seed [4]byte

	// crypto/rand.Read never returns an error: since Go 1.24 it panics instead, so
	// there is no failure path here that could keep a backup from starting.
	_, _ = rand.Read(seed[:])

	jobIDSaltCounter.Store(binary.BigEndian.Uint32(seed[:]) % jobIDSaltSpace)
}

// newJobID returns an identifier for a server-integrated backup job.
func newJobID() string {
	return newJobIDForTime(time.Now())
}

// newJobIDForTime returns an identifier for a server-integrated backup job, shaped as
// 260316T142035-k3f9: a UTC timestamp at second resolution followed by a base36 salt.
//
// It is in a separate function, for testing purposes.
func newJobIDForTime(now time.Time) string {
	buf := make([]byte, 0, jobIDLen)
	buf = now.UTC().AppendFormat(buf, jobIDTimeLayout)
	buf = append(buf, jobIDSeparator)

	// Emit the salt least significant digit first into a fixed-width array, so it is
	// zero padded rather than truncated to its natural width.
	var salt [jobIDSaltLen]byte

	v := jobIDSaltCounter.Add(1) % jobIDSaltSpace
	for i := jobIDSaltLen - 1; i >= 0; i-- {
		salt[i] = base36Digits[v%36]
		v /= 36
	}

	return string(append(buf, salt[:]...))
}

// IsJobID reports whether s has the form of a job id produced by this package.
// It lets storage listings tell backup folders apart from any other prefix.
func IsJobID(s string) bool {
	return jobIDRegexp.MatchString(s)
}
