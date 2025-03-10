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
	"strings"
	"time"

	"github.com/aerospike/backup-go/internal/util"
)

// DefaultPollWarmDuration is the interval between requests to cloud providers,
// to get file status during files restore.
const DefaultPollWarmDuration = time.Minute

// reader interface defines methods for listing and streaming objects
type reader interface {
	ListObjects(ctx context.Context, path string) ([]string, error)
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
