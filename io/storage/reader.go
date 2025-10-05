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

package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/backup-go/internal/util"
)

// DefaultPollWarmDuration is the interval between requests to cloud providers,
// to get file status during files restore.
const DefaultPollWarmDuration = time.Minute

// reader interface defines methods for listing and streaming objects
type reader interface {
	// ListObjects return list of filenames in given path. Result contains full path, from the root.
	ListObjects(ctx context.Context, path string) ([]string, error)
	// SetObjectsToStream sets elements that would be returned by StreamFiles method.
	SetObjectsToStream(list []string)
}

// PreSort performs pre-processing of backup files by sorting them before reading.
// It retrieves a list of objects from the specified path, sorts them according to the
// backup file naming conventions, and configures the reader to stream the sorted list.
// Returns an error if listing objects fails or if the sorting operation fails.
func PreSort(ctx context.Context, r reader, path string) error {
	// List all files first.
	list, err := r.ListObjects(ctx, path)
	if err != nil {
		return err
	}

	// Sort files.
	list, err = util.SortBackupFiles(list)
	if err != nil {
		return err
	}

	// Pass sorted list to reader.
	r.SetObjectsToStream(list)

	return nil
}

// CleanPath sanitizes the input path string based on the storage type (S3 or non-S3).
// For S3 storage, it removes the root path "/" as S3 uses empty string for root.
// For all storage types, it ensures proper trailing slash format except for empty or root paths.
// Returns the cleaned path string.
func CleanPath(path string, isS3 bool) string {
	if isS3 {
		// S3 storage can read/write to "/" prefix, so we should replace it with "".
		if path == "/" {
			return ""
		}
	}

	result := path
	if !strings.HasSuffix(path, "/") && path != "/" && path != "" {
		result = fmt.Sprintf("%s/", path)
	}

	return result
}

// IsDirectory determines if a given file name represents a directory within the specified prefix.
// It considers three cases:
//  1. File name ends with "/" (definite directory)
//  2. File name is within a prefix and contains "/" after the prefix
//  3. File name contains "/" (general case)
//
// Returns true if the file name represents a directory, false otherwise.
func IsDirectory(prefix, fileName string) bool {
	// If file name ends with / it is 100% dir.
	if strings.HasSuffix(fileName, "/") {
		return true
	}

	// If we look inside some folder.
	if strings.HasPrefix(fileName, prefix) {
		// For root folder we should add.
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		clean := strings.TrimPrefix(fileName, prefix)

		return strings.Contains(clean, "/")
	}
	// All other variants.
	return strings.Contains(fileName, "/")
}

// SkippedFiles collects information about files that were
// not processed during an operation.
type SkippedFiles struct {
	// prefixes holds a list of path prefixes used to identify files
	// that should be skipped.
	prefixes []string
	// filePaths stores a list of full paths for individual files that
	// were skipped.
	filePaths []string
	mu        sync.RWMutex
}

// NewSkippedFiles returns new SkippedFiles instance, to track skipped files.
func NewSkippedFiles(prefixes []string) *SkippedFiles {
	return &SkippedFiles{
		prefixes:  prefixes,
		filePaths: make([]string, 0),
	}
}

// Skip check if file must be skipped. If yes, it will be added to filePaths.
func (s *SkippedFiles) Skip(path string) bool {
	if s == nil {
		return false
	}

	if len(s.prefixes) == 0 {
		return false
	}

	fileName := filepath.Base(path)

	for i := range s.prefixes {
		if s.prefixes[i] == "" {
			// Skip empty prefixes
			continue
		}

		if strings.HasPrefix(fileName, s.prefixes[i]) {
			s.mu.Lock()
			s.filePaths = append(s.filePaths, path)
			s.mu.Unlock()

			return true
		}
	}

	return false
}

// GetSkipped returns a list of file paths that were skipped during the `StreamFlies` with skipPrefix.
func (s *SkippedFiles) GetSkipped() []string {
	if s == nil {
		return nil
	}

	if len(s.filePaths) == 0 {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// As slices are passed as pointer, we should create a copy before return.
	result := make([]string, len(s.filePaths))
	copy(result, s.filePaths)

	return result
}
