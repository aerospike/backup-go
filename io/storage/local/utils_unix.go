//go:build !windows

package local

func isWindowsNotDir(err error) bool {
	return false
}
