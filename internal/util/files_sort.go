package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const fileExt = ".asbx"

// backupFile is used for sorting files.
type backupFile struct {
	prefix int    // stores the number before _source.
	suffix int    // stores the number after _ns1_.
	name   string // original filename.
}

// parseFileName parse file name to backupFile struct.
func parseFileName(name string) (backupFile, error) {
	// Split by underscore to get parts.
	parts := strings.Split(name, "_")
	if len(parts) != 3 {
		return backupFile{}, fmt.Errorf("invalid filename format: %s", name)
	}

	// Parse prefix number (before _source).
	prefix, err := strconv.Atoi(parts[0])
	if err != nil {
		return backupFile{}, fmt.Errorf("invalid prefix number: %s", parts[0])
	}

	// Parse suffix number (after _ns1_).
	suffixWithExt := parts[2]

	suffix, err := strconv.Atoi(strings.TrimSuffix(suffixWithExt, fileExt))
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
	// Prepare strings for sorting.
	presort := make([][]backupFile, len(files))
	maxPrefix := 0

	for _, file := range files {
		f, err := parseFileName(file)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file name: %s", file)
		}
		// Set max prefix.
		if f.prefix > maxPrefix {
			maxPrefix = f.prefix
		}

		presort[f.prefix] = append(presort[f.prefix], f)
	}
	// Trim nils.
	presort = presort[:maxPrefix+1]

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