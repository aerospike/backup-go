package local

import (
	"errors"
	"strings"
	"syscall"
)

func isNotDir(err error) bool {
	if err == nil {
		return false
	}
	// Check Unix and Windows syscall errors
	if errors.Is(err, syscall.ENOTDIR) || isWindowsNotDir(err) {
		return true
	}
	// Fallback check for unexported error strings
	return strings.Contains(err.Error(), "not a directory")
}
