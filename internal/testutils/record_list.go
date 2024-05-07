package testutils

import (
	"bytes"
	"os"
	"path/filepath"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// Subtract one list from another
func Subtract(list1, list2 []*a.Record) (result []*a.Record) {
	for _, v := range list1 {
		if !contains(v, list2) {
			result = append(result, v)
		}
	}

	return
}

// contains checks whether a record is in the list
func contains(record *a.Record, list []*a.Record) bool {
	for _, v := range list {
		if bytes.Equal(record.Key.Digest(), v.Key.Digest()) {
			return true
		}
	}

	return false
}

func DirSize(path string) int64 {
	var size int64

	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}

		return err
	})

	return size
}
