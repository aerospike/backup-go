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

package util

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	FileExtAsbx = ".asbx"
	FileExtAsb  = ".asb"
)

// backupFile is used for sorting files.
type backupFile struct {
	prefix int    // stores the number before _source.
	suffix int    // stores the number after _ns1_.
	name   string // original filename.
}

// parseFileName parse file name to backupFile struct.
func parseFileName(name string) (backupFile, error) {
	fileName := filepath.Base(name)
	// Split by underscore to get parts.
	parts := strings.Split(fileName, "_")
	if len(parts) != 3 {
		return backupFile{}, fmt.Errorf("invalid filename format: %s", fileName)
	}

	// Parse prefix number (before _source).
	prefix, err := strconv.Atoi(parts[0])
	if err != nil {
		return backupFile{}, fmt.Errorf("invalid prefix number: %s", parts[0])
	}

	// Parse suffix number (after _ns1_).
	suffixWithExt := parts[2]

	suffix, err := strconv.Atoi(strings.TrimSuffix(suffixWithExt, FileExtAsbx))
	if err != nil {
		return backupFile{}, fmt.Errorf("invalid suffix number: %s", suffixWithExt)
	}

	return backupFile{
		prefix: prefix,
		suffix: suffix,
		name:   name,
	}, nil
}

// SortBackupFiles sort files for better restore performance.
func SortBackupFiles(files []string) ([]string, error) {
	if len(files) < 2 {
		return files, nil
	}

	// Prepare strings for sorting.
	presort := make(map[int][]backupFile)

	for _, file := range files {
		f, err := parseFileName(file)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file name %s: %w", file, err)
		}

		presort[f.prefix] = append(presort[f.prefix], f)
	}

	// sort each group.
	for o := range presort {
		sort.Slice(presort[o], func(i, j int) bool {
			return presort[o][i].suffix < presort[o][j].suffix
		})
	}

	result := make([]string, 0, len(files))

	// Iterator.
	i := 0
	// Number of empty subslices.
	e := 0

	for {
		var head backupFile
		// Try to get the first element.
		head, presort[i] = popFirst(presort[i])
		// If slice is not empty.
		if presort[i] != nil {
			// Append result.
			result = append(result, head.name)
		} else {
			// If slice empty, increase empty counter for this i iteration.
			e++
		}
		// If we hve more subslices in current iteration, increase i.
		if i < len(presort)-1 {
			i++
		} else {
			// If number of empty subslices = number of initial slices, we finished.
			if e == len(presort) {
				// So exit.
				break
			}
			// If we haven't finished yet, nullify counters and run again.
			i = 0
			e = 0
		}
	}

	return result, nil
}

// popFirst returns the first element from slice and removes it.
func popFirst(slice []backupFile) (first backupFile, remain []backupFile) {
	if len(slice) == 0 {
		return backupFile{}, nil
	}

	first = (slice)[0]
	slice = (slice)[1:]

	return first, slice
}
