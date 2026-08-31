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

package local

import (
	"bufio"
	"errors"
	"io"
)

// bufferedFile is a wrapper around a `bufio.Writer` and a `io.Closer`.
type bufferedFile struct {
	*bufio.Writer
	closer io.Closer
}

// Close flushes the writer and closes the closer.
func (bf *bufferedFile) Close() error {
	flushErr := bf.Flush()
	closeErr := bf.closer.Close()

	return errors.Join(flushErr, closeErr)
}
