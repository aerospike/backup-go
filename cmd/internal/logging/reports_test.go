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

package logging

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	bModels "github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndent(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "Empty key",
			key:      "",
			expected: ":" + strings.Repeat(" ", 21),
		},
		{
			name:     "Short key",
			key:      "Key",
			expected: "Key:" + strings.Repeat(" ", 18),
		},
		{
			name:     "Long key",
			key:      "ThisIsAVeryLongKey",
			expected: "ThisIsAVeryLongKey:" + strings.Repeat(" ", 3),
		},
		{
			name:     "Exact 20 character key",
			key:      "12345678901234567890",
			expected: "12345678901234567890:" + strings.Repeat(" ", 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := indent(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrintMetric(t *testing.T) {
	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Test with string value
	printMetric("TestKey", "TestValue")

	// Test with integer value
	printMetric("IntKey", 123)

	// Test with float value
	printMetric("FloatKey", 123.456)

	// Close writer and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	output := buf.String()

	// Verify output
	assert.Contains(t, output, "TestKey:"+strings.Repeat(" ", 21-len("TestKey"))+"TestValue")
	assert.Contains(t, output, "IntKey:"+strings.Repeat(" ", 21-len("IntKey"))+"123")
	assert.Contains(t, output, "FloatKey:"+strings.Repeat(" ", 21-len("FloatKey"))+"123.456")
}

func TestPrintBackupReport(t *testing.T) {
	// Create a backup stats object
	stats := bModels.NewBackupStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.BytesWritten.Add(5000000)
	stats.TotalRecords.Add(1000)
	stats.IncFiles() // Increment to 1
	stats.IncFiles() // Increment to 2
	stats.IncFiles() // Increment to 3
	stats.IncFiles() // Increment to 4
	stats.IncFiles() // Increment to 5
	stats.IncFiles() // Increment to 6
	stats.IncFiles() // Increment to 7
	stats.IncFiles() // Increment to 8
	stats.IncFiles() // Increment to 9
	stats.IncFiles() // Increment to 10

	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	printBackupReport(stats, false)

	// Close writer and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	output := buf.String()

	// Verify output
	assert.Contains(t, output, headerBackupReport)
	assert.Contains(t, output, "Start Time")
	assert.Contains(t, output, stats.StartTime.Format(time.RFC1123))
	assert.Contains(t, output, "Duration")
	assert.Contains(t, output, "Records Read")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "sIndex Read")
	assert.Contains(t, output, "5")
	assert.Contains(t, output, "UDFs Read")
	assert.Contains(t, output, "3")
	assert.Contains(t, output, "Bytes Written")
	assert.Contains(t, output, "5000000")
	assert.Contains(t, output, "Total Records")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "Files Written")
	assert.Contains(t, output, "10")
}

func TestPrintBackupReportXdr(t *testing.T) {
	// Create a backup stats object
	stats := bModels.NewBackupStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.BytesWritten.Add(5000000)
	stats.TotalRecords.Add(1000)
	stats.IncFiles() // Increment to 1
	stats.IncFiles() // Increment to 2
	stats.IncFiles() // Increment to 3
	stats.IncFiles() // Increment to 4
	stats.IncFiles() // Increment to 5
	stats.IncFiles() // Increment to 6
	stats.IncFiles() // Increment to 7
	stats.IncFiles() // Increment to 8
	stats.IncFiles() // Increment to 9
	stats.IncFiles() // Increment to 10

	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function with isXdr=true
	printBackupReport(stats, true)

	// Close writer and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	output := buf.String()

	// Verify output
	assert.Contains(t, output, headerBackupReport)
	assert.Contains(t, output, "Records Received")
	assert.NotContains(t, output, "Records Read")
}

func TestLogBackupReport(t *testing.T) {
	// Create a backup stats object
	stats := bModels.NewBackupStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.BytesWritten.Add(5000000)
	stats.TotalRecords.Add(1000)
	stats.IncFiles() // Increment to 1
	stats.IncFiles() // Increment to 2
	stats.IncFiles() // Increment to 3
	stats.IncFiles() // Increment to 4
	stats.IncFiles() // Increment to 5
	stats.IncFiles() // Increment to 6
	stats.IncFiles() // Increment to 7
	stats.IncFiles() // Increment to 8
	stats.IncFiles() // Increment to 9
	stats.IncFiles() // Increment to 10

	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Call the function
	logBackupReport(stats, false, logger)

	// Verify log output
	logOutput := buf.String()
	assert.Contains(t, logOutput, "backup report")
	assert.Contains(t, logOutput, "start_time=")
	assert.Contains(t, logOutput, "duration=")
	assert.Contains(t, logOutput, "records_read=1000")
	assert.Contains(t, logOutput, "s_index_read=5")
	assert.Contains(t, logOutput, "udf_read=3")
	assert.Contains(t, logOutput, "bytes_written=5000000")
	assert.Contains(t, logOutput, "total_records=1000")
	assert.Contains(t, logOutput, "files_written=10")
}

func TestLogBackupReportXdr(t *testing.T) {
	// Create a backup stats object
	stats := bModels.NewBackupStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.BytesWritten.Add(5000000)
	stats.TotalRecords.Add(1000)
	stats.IncFiles() // Increment to 1
	stats.IncFiles() // Increment to 2
	stats.IncFiles() // Increment to 3
	stats.IncFiles() // Increment to 4
	stats.IncFiles() // Increment to 5
	stats.IncFiles() // Increment to 6
	stats.IncFiles() // Increment to 7
	stats.IncFiles() // Increment to 8
	stats.IncFiles() // Increment to 9
	stats.IncFiles() // Increment to 10

	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Call the function with isXdr=true
	logBackupReport(stats, true, logger)

	// Verify log output
	logOutput := buf.String()
	assert.Contains(t, logOutput, "backup report")
	assert.Contains(t, logOutput, "records_received=1000")
	assert.NotContains(t, logOutput, "records_read=")
}

func TestReportBackup(t *testing.T) {
	// Create a backup stats object
	stats := bModels.NewBackupStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.BytesWritten.Add(5000000)
	stats.TotalRecords.Add(1000)
	stats.IncFiles() // Increment to 1
	stats.IncFiles() // Increment to 2
	stats.IncFiles() // Increment to 3
	stats.IncFiles() // Increment to 4
	stats.IncFiles() // Increment to 5
	stats.IncFiles() // Increment to 6
	stats.IncFiles() // Increment to 7
	stats.IncFiles() // Increment to 8
	stats.IncFiles() // Increment to 9
	stats.IncFiles() // Increment to 10

	// Test with isJSON=false
	t.Run("Console output", func(t *testing.T) {
		// Redirect stdout to capture output
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		// Call the function
		ReportBackup(stats, false, false, nil)

		// Close writer and restore stdout
		w.Close()
		os.Stdout = oldStdout

		// Read captured output
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		require.NoError(t, err)
		output := buf.String()

		// Verify output
		assert.Contains(t, output, headerBackupReport)
		assert.Contains(t, output, "Records Read")
	})

	// Test with isJSON=true
	t.Run("JSON output", func(t *testing.T) {
		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		// Call the function
		ReportBackup(stats, false, true, logger)

		// Verify log output
		logOutput := buf.String()
		assert.Contains(t, logOutput, "backup report")
		assert.Contains(t, logOutput, "records_read=1000")
	})
}

func TestPrintRestoreReport(t *testing.T) {
	// Create a restore stats object
	stats := bModels.NewRestoreStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.RecordsExpired.Add(10)
	stats.RecordsSkipped.Add(20)
	stats.RecordsIgnored.Add(30)
	stats.IncrRecordsFresher()  // 1
	stats.IncrRecordsFresher()  // 2
	stats.IncrRecordsFresher()  // 3
	stats.IncrRecordsFresher()  // 4
	stats.IncrRecordsFresher()  // 5
	stats.IncrRecordsFresher()  // 6
	stats.IncrRecordsFresher()  // 7
	stats.IncrRecordsFresher()  // 8
	stats.IncrRecordsFresher()  // 9
	stats.IncrRecordsFresher()  // 10
	stats.IncrRecordsFresher()  // 11
	stats.IncrRecordsFresher()  // 12
	stats.IncrRecordsFresher()  // 13
	stats.IncrRecordsFresher()  // 14
	stats.IncrRecordsFresher()  // 15
	stats.IncrRecordsFresher()  // 16
	stats.IncrRecordsFresher()  // 17
	stats.IncrRecordsFresher()  // 18
	stats.IncrRecordsFresher()  // 19
	stats.IncrRecordsFresher()  // 20
	stats.IncrRecordsFresher()  // 21
	stats.IncrRecordsFresher()  // 22
	stats.IncrRecordsFresher()  // 23
	stats.IncrRecordsFresher()  // 24
	stats.IncrRecordsFresher()  // 25
	stats.IncrRecordsFresher()  // 26
	stats.IncrRecordsFresher()  // 27
	stats.IncrRecordsFresher()  // 28
	stats.IncrRecordsFresher()  // 29
	stats.IncrRecordsFresher()  // 30
	stats.IncrRecordsFresher()  // 31
	stats.IncrRecordsFresher()  // 32
	stats.IncrRecordsFresher()  // 33
	stats.IncrRecordsFresher()  // 34
	stats.IncrRecordsFresher()  // 35
	stats.IncrRecordsFresher()  // 36
	stats.IncrRecordsFresher()  // 37
	stats.IncrRecordsFresher()  // 38
	stats.IncrRecordsFresher()  // 39
	stats.IncrRecordsFresher()  // 40
	stats.IncrRecordsExisted()  // 1
	stats.IncrRecordsExisted()  // 2
	stats.IncrRecordsExisted()  // 3
	stats.IncrRecordsExisted()  // 4
	stats.IncrRecordsExisted()  // 5
	stats.IncrRecordsExisted()  // 6
	stats.IncrRecordsExisted()  // 7
	stats.IncrRecordsExisted()  // 8
	stats.IncrRecordsExisted()  // 9
	stats.IncrRecordsExisted()  // 10
	stats.IncrRecordsExisted()  // 11
	stats.IncrRecordsExisted()  // 12
	stats.IncrRecordsExisted()  // 13
	stats.IncrRecordsExisted()  // 14
	stats.IncrRecordsExisted()  // 15
	stats.IncrRecordsExisted()  // 16
	stats.IncrRecordsExisted()  // 17
	stats.IncrRecordsExisted()  // 18
	stats.IncrRecordsExisted()  // 19
	stats.IncrRecordsExisted()  // 20
	stats.IncrRecordsExisted()  // 21
	stats.IncrRecordsExisted()  // 22
	stats.IncrRecordsExisted()  // 23
	stats.IncrRecordsExisted()  // 24
	stats.IncrRecordsExisted()  // 25
	stats.IncrRecordsExisted()  // 26
	stats.IncrRecordsExisted()  // 27
	stats.IncrRecordsExisted()  // 28
	stats.IncrRecordsExisted()  // 29
	stats.IncrRecordsExisted()  // 30
	stats.IncrRecordsExisted()  // 31
	stats.IncrRecordsExisted()  // 32
	stats.IncrRecordsExisted()  // 33
	stats.IncrRecordsExisted()  // 34
	stats.IncrRecordsExisted()  // 35
	stats.IncrRecordsExisted()  // 36
	stats.IncrRecordsExisted()  // 37
	stats.IncrRecordsExisted()  // 38
	stats.IncrRecordsExisted()  // 39
	stats.IncrRecordsExisted()  // 40
	stats.IncrRecordsExisted()  // 41
	stats.IncrRecordsExisted()  // 42
	stats.IncrRecordsExisted()  // 43
	stats.IncrRecordsExisted()  // 44
	stats.IncrRecordsExisted()  // 45
	stats.IncrRecordsExisted()  // 46
	stats.IncrRecordsExisted()  // 47
	stats.IncrRecordsExisted()  // 48
	stats.IncrRecordsExisted()  // 49
	stats.IncrRecordsExisted()  // 50
	stats.IncrRecordsInserted() // 1
	stats.IncrRecordsInserted() // 2
	stats.IncrRecordsInserted() // 3
	stats.IncrRecordsInserted() // 4
	stats.IncrRecordsInserted() // 5
	stats.IncrRecordsInserted() // 6
	stats.IncrRecordsInserted() // 7
	stats.IncrRecordsInserted() // 8
	stats.IncrRecordsInserted() // 9
	stats.IncrRecordsInserted() // 10
	stats.IncrRecordsInserted() // 11
	stats.IncrRecordsInserted() // 12
	stats.IncrRecordsInserted() // 13
	stats.IncrRecordsInserted() // 14
	stats.IncrRecordsInserted() // 15
	stats.IncrRecordsInserted() // 16
	stats.IncrRecordsInserted() // 17
	stats.IncrRecordsInserted() // 18
	stats.IncrRecordsInserted() // 19
	stats.IncrRecordsInserted() // 20
	stats.IncrRecordsInserted() // 21
	stats.IncrRecordsInserted() // 22
	stats.IncrRecordsInserted() // 23
	stats.IncrRecordsInserted() // 24
	stats.IncrRecordsInserted() // 25
	stats.IncrRecordsInserted() // 26
	stats.IncrRecordsInserted() // 27
	stats.IncrRecordsInserted() // 28
	stats.IncrRecordsInserted() // 29
	stats.IncrRecordsInserted() // 30
	stats.IncrRecordsInserted() // 31
	stats.IncrRecordsInserted() // 32
	stats.IncrRecordsInserted() // 33
	stats.IncrRecordsInserted() // 34
	stats.IncrRecordsInserted() // 35
	stats.IncrRecordsInserted() // 36
	stats.IncrRecordsInserted() // 37
	stats.IncrRecordsInserted() // 38
	stats.IncrRecordsInserted() // 39
	stats.IncrRecordsInserted() // 40
	stats.IncrRecordsInserted() // 41
	stats.IncrRecordsInserted() // 42
	stats.IncrRecordsInserted() // 43
	stats.IncrRecordsInserted() // 44
	stats.IncrRecordsInserted() // 45
	stats.IncrRecordsInserted() // 46
	stats.IncrRecordsInserted() // 47
	stats.IncrRecordsInserted() // 48
	stats.IncrRecordsInserted() // 49
	stats.IncrRecordsInserted() // 50
	stats.IncrRecordsInserted() // 51
	stats.IncrRecordsInserted() // 52
	stats.IncrRecordsInserted() // 53
	stats.IncrRecordsInserted() // 54
	stats.IncrRecordsInserted() // 55
	stats.IncrRecordsInserted() // 56
	stats.IncrRecordsInserted() // 57
	stats.IncrRecordsInserted() // 58
	stats.IncrRecordsInserted() // 59
	stats.IncrRecordsInserted() // 60
	stats.IncrErrorsInDoubt()   // 1
	stats.IncrErrorsInDoubt()   // 2
	stats.IncrErrorsInDoubt()   // 3
	stats.IncrErrorsInDoubt()   // 4
	stats.IncrErrorsInDoubt()   // 5
	stats.TotalBytesRead.Add(5000000)

	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	printRestoreReport(stats)

	// Close writer and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	output := buf.String()

	// Verify output
	assert.Contains(t, output, headerRestoreReport)
	assert.Contains(t, output, "Start Time")
	assert.Contains(t, output, stats.StartTime.Format(time.RFC1123))
	assert.Contains(t, output, "Duration")
	assert.Contains(t, output, "Records Read")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "sIndex Read")
	assert.Contains(t, output, "5")
	assert.Contains(t, output, "UDFs Read")
	assert.Contains(t, output, "3")
	assert.Contains(t, output, "Expired Records")
	assert.Contains(t, output, "10")
	assert.Contains(t, output, "Skipped Records")
	assert.Contains(t, output, "20")
	assert.Contains(t, output, "Ignored Records")
	assert.Contains(t, output, "30")
	assert.Contains(t, output, "Fresher Records")
	assert.Contains(t, output, "40")
	assert.Contains(t, output, "Existed Records")
	assert.Contains(t, output, "50")
	assert.Contains(t, output, "Inserted Records")
	assert.Contains(t, output, "60")
	assert.Contains(t, output, "In Doubt Errors")
	assert.Contains(t, output, "5")
	assert.Contains(t, output, "Total Bytes Read")
	assert.Contains(t, output, "5000000")
}

func TestLogRestoreReport(t *testing.T) {
	// Create a restore stats object
	stats := bModels.NewRestoreStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.RecordsExpired.Add(10)
	stats.RecordsSkipped.Add(20)
	stats.RecordsIgnored.Add(30)
	for i := 0; i < 40; i++ {
		stats.IncrRecordsFresher()
	}
	for i := 0; i < 50; i++ {
		stats.IncrRecordsExisted()
	}
	for i := 0; i < 60; i++ {
		stats.IncrRecordsInserted()
	}
	for i := 0; i < 5; i++ {
		stats.IncrErrorsInDoubt()
	}
	stats.TotalBytesRead.Add(5000000)

	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Call the function
	logRestoreReport(stats, logger)

	// Verify log output
	logOutput := buf.String()
	assert.Contains(t, logOutput, "restore report")
	assert.Contains(t, logOutput, "start_time=")
	assert.Contains(t, logOutput, "duration=")
	assert.Contains(t, logOutput, "records_read=1000")
	assert.Contains(t, logOutput, "s_index_read=5")
	assert.Contains(t, logOutput, "udf_read=3")
	assert.Contains(t, logOutput, "expired_records=10")
	assert.Contains(t, logOutput, "skipped_records=20")
	assert.Contains(t, logOutput, "ignored_records=30")
	assert.Contains(t, logOutput, "fresher_records=40")
	assert.Contains(t, logOutput, "existed_records=50")
	assert.Contains(t, logOutput, "inserted_records=60")
	assert.Contains(t, logOutput, "in_doubt_errors=5")
	assert.Contains(t, logOutput, "total_bytes_read=5000000")
}

func TestReportRestore(t *testing.T) {
	// Create a restore stats object
	stats := bModels.NewRestoreStats()
	stats.StartTime = time.Now().Add(-1 * time.Hour) // 1 hour ago
	stats.ReadRecords.Add(1000)
	stats.AddSIndexes(5)
	stats.AddUDFs(3)
	stats.RecordsExpired.Add(10)
	stats.RecordsSkipped.Add(20)
	stats.RecordsIgnored.Add(30)
	for i := 0; i < 40; i++ {
		stats.IncrRecordsFresher()
	}
	for i := 0; i < 50; i++ {
		stats.IncrRecordsExisted()
	}
	for i := 0; i < 60; i++ {
		stats.IncrRecordsInserted()
	}
	for i := 0; i < 5; i++ {
		stats.IncrErrorsInDoubt()
	}
	stats.TotalBytesRead.Add(5000000)

	// Test with isJSON=false
	t.Run("Console output", func(t *testing.T) {
		// Redirect stdout to capture output
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		// Call the function
		ReportRestore(stats, false, nil)

		// Close writer and restore stdout
		w.Close()
		os.Stdout = oldStdout

		// Read captured output
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		require.NoError(t, err)
		output := buf.String()

		// Verify output
		assert.Contains(t, output, headerRestoreReport)
		assert.Contains(t, output, "Records Read")
	})

	// Test with isJSON=true
	t.Run("JSON output", func(t *testing.T) {
		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		// Call the function
		ReportRestore(stats, true, logger)

		// Verify log output
		logOutput := buf.String()
		assert.Contains(t, logOutput, "restore report")
		assert.Contains(t, logOutput, "records_read=1000")
	})
}

func TestPrintEstimateReport(t *testing.T) {
	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	printEstimateReport(5000000)

	// Close writer and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	output := buf.String()

	// Verify output
	assert.Contains(t, output, headerEstimateReport)
	assert.Contains(t, output, "File size (bytes)")
	assert.Contains(t, output, "5000000")
}

func TestLogEstimateReport(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Call the function
	logEstimateReport(5000000, logger)

	// Verify log output
	logOutput := buf.String()
	assert.Contains(t, logOutput, "estimate report")
	assert.Contains(t, logOutput, "file_size_bytes=5000000")
}

func TestReportEstimate(t *testing.T) {
	// Test with isJSON=false
	t.Run("Console output", func(t *testing.T) {
		// Redirect stdout to capture output
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		// Call the function
		ReportEstimate(5000000, false, nil)

		// Close writer and restore stdout
		w.Close()
		os.Stdout = oldStdout

		// Read captured output
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		require.NoError(t, err)
		output := buf.String()

		// Verify output
		assert.Contains(t, output, headerEstimateReport)
		assert.Contains(t, output, "File size (bytes)")
		assert.Contains(t, output, "5000000")
	})

	// Test with isJSON=true
	t.Run("JSON output", func(t *testing.T) {
		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		// Call the function
		ReportEstimate(5000000, true, logger)

		// Verify log output
		logOutput := buf.String()
		assert.Contains(t, logOutput, "estimate report")
		assert.Contains(t, logOutput, "file_size_bytes=5000000")
	})
}
