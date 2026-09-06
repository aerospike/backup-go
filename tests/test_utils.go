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

package tests

import (
	"bytes"
	"os"
	"path/filepath"

	a "github.com/aerospike/aerospike-client-go/v8"
)

// Subtract returns a new list containing elements of list1 that are
// not contained in list2.
func Subtract(list1, list2 []*a.Record) (result []*a.Record) {
	for _, v := range list1 {
		if !contains(v, list2) {
			result = append(result, v)
		}
	}

	return
}

// contains checks whether a record is in the list.
func contains(record *a.Record, list []*a.Record) bool {
	for _, v := range list {
		if bytes.Equal(record.Key.Digest(), v.Key.Digest()) {
			return true
		}
	}

	return false
}

// DirSize returns the size, in bytes, of the directory specified by the given path.
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
