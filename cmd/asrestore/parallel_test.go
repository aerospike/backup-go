package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// Record represents your data structure
type Record struct {
	Key  string
	Data []byte
}

// generateTestFiles creates test files with random data
func generateTestFiles(numFiles, recordsPerFile int) ([]string, error) {
	var files []string
	tmpDir := os.TempDir()

	for i := 0; i < numFiles; i++ {
		filename := filepath.Join(tmpDir, fmt.Sprintf("test_file_%d.dat", i))
		f, err := os.Create(filename)
		if err != nil {
			return nil, err
		}

		// Write random data
		data := make([]byte, 1024) // 1KB per record
		for j := 0; j < recordsPerFile; j++ {
			rand.Read(data)
			f.Write(data)
		}

		f.Close()
		files = append(files, filename)
	}

	return files, nil
}

// cleanup removes test files
func cleanup(files []string) {
	for _, f := range files {
		os.Remove(f)
	}
}

// MockDB simulates Aerospike for testing
type MockDB struct {
	writeDelay time.Duration // Simulate network latency
}

func (m *MockDB) Write(records []Record) error {
	time.Sleep(m.writeDelay)
	return nil
}

// processFiles is your actual processing function
func processFiles(files []string, parallel int, db *MockDB) error {
	filesChan := make(chan string, parallel)
	resultsChan := make(chan []Record, parallel)
	errChan := make(chan error, parallel)
	done := make(chan struct{})

	// Start file readers
	for i := 0; i < parallel; i++ {
		go func() {
			for _ = range filesChan {
				// Simulate file reading
				time.Sleep(50 * time.Millisecond)
				records := make([]Record, 10)
				resultsChan <- records
			}
		}()
	}

	// Start DB writers
	for i := 0; i < parallel; i++ {
		go func() {
			for records := range resultsChan {
				if err := db.Write(records); err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	// Feed files to workers
	go func() {
		for _, file := range files {
			filesChan <- file
		}
		close(filesChan)
		close(done)
	}()

	// Wait for completion or error
	select {
	case err := <-errChan:
		return err
	case <-done:
		return nil
	}
}

func BenchmarkParallelProcessing(b *testing.B) {
	// Test different levels of parallelism
	parallelLevels := []int{1, 2, 4, 6, 8, 12, 16}
	numFiles := 100
	recordsPerFile := 1000

	// Generate test files
	files, err := generateTestFiles(numFiles, recordsPerFile)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup(files)

	// Create mock DB with simulated latency
	db := &MockDB{writeDelay: 10 * time.Millisecond}

	for _, parallel := range parallelLevels {
		b.Run(fmt.Sprintf("Parallel-%d", parallel), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := processFiles(files, parallel, db)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWithRealAerospike tests with actual Aerospike connection
func BenchmarkWithRealAerospike(b *testing.B) {
	// Configure your Aerospike connection
	client, err := aero.NewClient("localhost", 3000)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	// Rest of the benchmark implementation...
	// Similar to above but with real Aerospike operations
}

// To run the benchmark:
// go test -bench=. -benchmem ./...
