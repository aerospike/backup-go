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

package instrumental

import (
	"io"
	"log/slog"
	"time"
)

// Writer wraps an io.WriteCloser and measures write performance.
//
// Metrics tracked:
//   - runTime      = activeTime + idleTime
//   - activeTime   = time spent writing or closing
//   - idleTime     = time between writes (gaps of inactivity)
//   - bytesWritten = total number of bytes written
//   - writeCount   = total number of Write() calls
//   - avgWriteSize = bytesWritten / writeCount
//   - activeSpeed  = bytesWritten / activeTime (bytes/sec while actively writing)
//   - overallSpeed = bytesWritten / runTime (bytes/sec over total lifetime)
//   - active%      = activeTime / runTime * 100
type Writer struct {
	writer io.WriteCloser

	startTime        time.Time
	lastWriteEndTime time.Time
	activeTime       time.Duration
	idleTime         time.Duration
	runTime          time.Duration
	bytesWritten     int64
	writeCount       int64
}

// NewWriter creates a new Writer.
func NewWriter(w io.WriteCloser) *Writer {
	now := time.Now()
	return &Writer{
		writer:           w,
		startTime:        now,
		lastWriteEndTime: now,
	}
}

// Write writes data to the underlying writer and tracks time and bytes.
func (iw *Writer) Write(p []byte) (n int, err error) {
	now := time.Now()
	iw.idleTime += now.Sub(iw.lastWriteEndTime)

	writeStartTime := now
	n, err = iw.writer.Write(p)
	writeEndTime := time.Now()

	iw.activeTime += writeEndTime.Sub(writeStartTime)
	iw.lastWriteEndTime = writeEndTime

	iw.bytesWritten += int64(n)
	iw.writeCount++

	return n, err
}

// Close closes the underlying writer and logs statistics.
func (iw *Writer) Close() error {
	closeStartTime := time.Now()
	err := iw.writer.Close()
	iw.activeTime += time.Since(closeStartTime)

	iw.runTime = time.Since(iw.startTime)

	activePercentage := 0.0
	if iw.runTime > 0 {
		activePercentage = float64(iw.activeTime) / float64(iw.runTime) * 100
	}

	activeWriteSpeed := 0.0
	if iw.activeTime > 0 {
		activeWriteSpeed = float64(iw.bytesWritten) / iw.activeTime.Seconds()
	}

	overallWriteSpeed := 0.0
	if iw.runTime > 0 {
		overallWriteSpeed = float64(iw.bytesWritten) / iw.runTime.Seconds()
	}

	avgBytesPerWrite := 0.0
	if iw.writeCount > 0 {
		avgBytesPerWrite = float64(iw.bytesWritten) / float64(iw.writeCount)
	}

	slog.Info("Writer Stats",
		slog.Int64("bytes_written", iw.bytesWritten),
		slog.Int64("write_count", iw.writeCount),
		slog.Float64("avg_write_size_bytes", avgBytesPerWrite),
		slog.Float64("write_speed_active_bytes_per_sec", activeWriteSpeed),
		slog.Float64("write_speed_overall_bytes_per_sec", overallWriteSpeed),
		slog.Duration("active_time", iw.activeTime),
		slog.Duration("idle_time", iw.idleTime),
		slog.Duration("total_run_time", iw.runTime),
		slog.Float64("active_percentage", activePercentage),
	)

	return err
}
