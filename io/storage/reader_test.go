package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReader_isDirectory(t *testing.T) {
	prefix := "/"
	fileNames := []string{
		"test/innerfldr/",
		"test/innerfldr/test_inner.asb",
		"test/test.asb",
		"test/test2.asb",
		"test3.asb",
	}
	var dirCounter int
	for i := range fileNames {
		if IsDirectory(prefix, fileNames[i]) {
			dirCounter++
		}
	}
	require.Equal(t, 4, dirCounter)
}
