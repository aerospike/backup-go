package common

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateFilename ensures that the filename is a single, safe path component
// without directory traversal characters, absolute paths, or null bytes.
func ValidateFilename(filename string) error {
	if filename == "" {
		return nil
	}

	if filename == "." ||
		filename == ".." ||
		strings.ContainsAny(filename, `/\`) ||
		strings.ContainsRune(filename, '\x00') ||
		filepath.IsAbs(filename) ||
		filepath.VolumeName(filename) != "" {
		return fmt.Errorf("filename must be a single portable path component: %q", filename)
	}

	return nil
}
