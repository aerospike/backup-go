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

// Package segment decodes Aerospike server side backup segment files. A segment
// is a sequence of rblock aligned flat records followed by zero slack; the
// package parses every record so a caller can prove that a backup is readable
// without restoring it.
package segment

import "fmt"

// Stats summarizes a validated backup segment.
type Stats struct {
	// RecordCount is the number of fully parsed records.
	RecordCount int
	// SkippedCompressed is the number of compressed records that were walked
	// over without decoding. Compression is not produced by the server today;
	// the counter exists so it does not silently look like data loss when it is.
	SkippedCompressed int
	// ByteCount is the number of bytes covered by records, slack excluded.
	ByteCount int
	// SlackBytes is the size of the zero filled tail after the last record.
	SlackBytes int
}

// Validate parses data as a backup segment and returns what it found.
//
// Every record is checked: header, metadata, bins, end marker and padding. The
// first broken record aborts the walk and is reported as a *RecordError that
// wraps one of the package sentinels, so both errors.Is and errors.As work.
func Validate(data []byte) (Stats, error) {
	var stats Stats

	if len(data) == 0 {
		return stats, ErrEmptySegment
	}

	var (
		off   int
		index int
	)

	for {
		remain := len(data) - off
		if remain < minRecordSize {
			if !isZero(data[off:]) {
				return stats, newRecordError(index, off, ErrBadTailSlack)
			}

			stats.SlackBytes = remain

			return finish(stats)
		}

		hdr, err := parseFlatHeader(data[off:])
		if err != nil {
			return stats, newRecordError(index, off, err)
		}

		if hdr.magic != flatMagic {
			// A zero filled tail longer than one record is regular padding,
			// not a corrupt record.
			if isZero(data[off:]) {
				stats.SlackBytes = remain

				return finish(stats)
			}

			return stats, newRecordError(index, off,
				fmt.Errorf("%w: 0x%08x", ErrBadMagic, hdr.magic))
		}

		size := hdr.recordSize()

		switch {
		case size < minRecordSize:
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: %d", ErrRecordTooSmall, size))
		case off+size > len(data):
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: size %d, remain %d", ErrRecordOutOfBounds, size, remain))
		}

		if hdr.isCompressed {
			stats.SkippedCompressed++
		} else {
			if err := validateRecord(hdr, data, off, size); err != nil {
				return stats, newRecordError(index, off, err)
			}

			stats.RecordCount++
		}

		stats.ByteCount += size
		off += size
		index++
	}
}

// finish rejects a payload that turned out to hold no record at all.
func finish(stats Stats) (Stats, error) {
	if stats.RecordCount == 0 && stats.SkippedCompressed == 0 {
		return stats, ErrNoRecords
	}

	return stats, nil
}
