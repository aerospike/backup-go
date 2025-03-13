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

package counter

import (
	"io"
	"sync/atomic"
)

// Writer counts total number of bytes written with it.
// We need it to get actual number of bytes written, after compression and encryption.
type Writer struct {
	writer io.WriteCloser
	total  *atomic.Uint64
	// This counter is used per backup file.
	// It is reset every time a new file is created.
	fileSize *atomic.Uint64
}

func NewWriter(w io.WriteCloser, total, fileSize *atomic.Uint64) *Writer {
	return &Writer{
		writer:   w,
		total:    total,
		fileSize: fileSize,
	}
}

func (cw *Writer) Write(p []byte) (n int, err error) {
	n, err = cw.writer.Write(p)
	cw.total.Add(uint64(n))

	if cw.fileSize != nil {
		cw.fileSize.Add(uint64(n))
	}

	return n, err
}

func (cw *Writer) Close() error {
	return cw.writer.Close()
}
