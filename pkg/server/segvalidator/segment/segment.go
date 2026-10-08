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

// Package segment decodes Aerospike server side backup segment files. A segment
// is an ASBK frame around a body of tightly packed, rblock aligned flat
// records; the package checks the frame and parses every record the way the
// server does on restore, so a caller can prove that a backup is restorable
// without restoring it.
package segment

import "fmt"

// PartitionCount is the number of partitions of every namespace.
const PartitionCount = 4096

// Stats summarizes a validated backup segment.
type Stats struct {
	// RecordCount is the number of fully parsed records.
	RecordCount int
	// ByteCount is the number of body bytes covered by records, the frame
	// excluded.
	ByteCount int
}

// Option configures Validate.
type Option interface {
	apply(*options)
}

// options holds what the caller knows about the segment beyond its bytes.
type options struct {
	partition      int
	checkPartition bool
}

// partitionOption is the Option WithPartition returns.
type partitionOption int

func (p partitionOption) apply(o *options) {
	o.partition = int(p)
	o.checkPartition = true
}

// WithPartition requires every record of the segment to belong to partition
// id. The server restores a query stream segment into the partition of the
// manifest naming it, and aborts the restore on a record of any other
// partition. Change stream segments mix partitions and must not use it. An id
// outside 0..PartitionCount-1 makes Validate fail with ErrInvalidPartition.
func WithPartition(id int) Option {
	return partitionOption(id)
}

// Validate parses data as a backup segment object and returns what it found.
//
// The frame is checked first, then every record of the body: header, metadata,
// bins, end marker and padding. The body must end exactly on a record
// boundary, and the number of records must match the frame footer. A
// compressed record fails the segment, because the server refuses to restore
// it.
//
// The first broken record aborts the walk and is reported as a *RecordError
// that wraps one of the package sentinels, so both errors.Is and errors.As
// work. Its offset is relative to the start of the segment object. Frame
// errors are not tied to a record: they wrap a package sentinel directly
// instead of a *RecordError.
//
// The returned Stats are filled even when the segment fails: they cover the
// records parsed before the failure, or every record when only the footer
// count disagrees.
func Validate(data []byte, opts ...Option) (Stats, error) {
	var o options
	for _, opt := range opts {
		opt.apply(&o)
	}

	if o.checkPartition && (o.partition < 0 || o.partition >= PartitionCount) {
		return Stats{}, fmt.Errorf("%w: %d", ErrInvalidPartition, o.partition)
	}

	fr, err := parseFrame(data)
	if err != nil {
		return Stats{}, err
	}

	stats, err := walkBody(data[:fr.bodyEnd()], fr.bodyOff, o)
	if err != nil {
		return stats, err
	}

	if parsed := uint64(stats.RecordCount); parsed != fr.recordCount {
		return stats, fmt.Errorf("%w: footer %d, parsed %d",
			ErrRecordCountMismatch, fr.recordCount, parsed)
	}

	return stats, nil
}

// walkBody parses the records in data[off:]. data must end where the body
// ends, so no record can reach into the footer.
func walkBody(data []byte, off int, o options) (Stats, error) {
	var stats Stats

	for index := 0; off < len(data); index++ {
		remain := len(data) - off
		// The body is tightly packed, so any residue too short to hold a
		// record means the segment was cut inside one, whatever its content.
		if remain < minRecordSize {
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: %d bytes left", ErrTruncatedRecord, remain))
		}

		hdr, err := parseFlatHeader(data[off:])
		if err != nil {
			return stats, newRecordError(index, off, err)
		}

		if hdr.magic != flatMagic {
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: 0x%08x", ErrBadMagic, hdr.magic))
		}

		size := hdr.recordSize()

		switch {
		case size < minRecordSize:
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: %d", ErrRecordTooSmall, size))
		case size > remain:
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: size %d, remain %d", ErrRecordOutOfBounds, size, remain))
		}

		if !endMarkInFrame(data[off : off+size]) {
			return stats, newRecordError(index, off,
				fmt.Errorf("%w: size %d", ErrFrameContentMismatch, size))
		}

		if o.checkPartition {
			if p := recordPartition(data[off:]); p != o.partition {
				return stats, newRecordError(index, off,
					fmt.Errorf("%w: record %d, segment %d", ErrWrongPartition, p, o.partition))
			}
		}

		if hdr.isCompressed {
			return stats, newRecordError(index, off, ErrCompressedRecord)
		}

		if err := validateRecord(hdr, data, off, size); err != nil {
			return stats, newRecordError(index, off, err)
		}

		stats.RecordCount++
		stats.ByteCount += size
		off += size
	}

	return stats, nil
}
