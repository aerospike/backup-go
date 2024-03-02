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

package backup

import (
	"fmt"
	"runtime/debug"
)

func handlePanic(errors chan<- error) {
	if r := recover(); r != nil {
		var err error

		panicMsg := "a backup operation has panicked:"
		if _, ok := r.(error); ok {
			err = fmt.Errorf(panicMsg+" caused by this error: \"%w\"\n", r.(error))
		} else {
			err = fmt.Errorf(panicMsg+" caused by: \"%v\"\n", r)
		}

		err = fmt.Errorf("%w, with stacktrace: \"%s\"", err, debug.Stack())

		errors <- err
	}
}
