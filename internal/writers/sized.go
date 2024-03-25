// Copyright 2024-2024 Aerospike, Inc.
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

package writers

import "io"

// Sized wraps a io.WriteCloser and adds a size limit.
// when the size limit is reached, the io.WriteCloser is closed and a new one is created
// using the open function.
type Sized struct {
	io.WriteCloser
	open  func() (io.WriteCloser, error)
	size  int64
	limit int64
}

// NewSized creates a new Sized writer with a size limit.
// limit must be greater than 0.
func NewSized(limit int64, writer io.WriteCloser, open func() (io.WriteCloser, error)) *Sized {
	if limit <= 0 {
		panic("limit must be greater than 0")
	}

	return &Sized{
		limit:       limit,
		open:        open,
		WriteCloser: writer,
	}
}

func (f *Sized) Write(p []byte) (n int, err error) {
	if f.size >= f.limit {
		f.WriteCloser.Close()

		f.size = 0

		f.WriteCloser, err = f.open()
		if err != nil {
			return 0, err
		}
	}

	n, err = f.WriteCloser.Write(p)
	f.size += int64(n)

	return n, err
}
