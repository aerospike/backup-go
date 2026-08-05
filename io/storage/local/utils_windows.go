//go:build windows

package local

import (
	"errors"
	"syscall"
)

func isWindowsNotDir(err error) bool {
	return errors.Is(err, syscall.ERROR_DIRECTORY)
}
