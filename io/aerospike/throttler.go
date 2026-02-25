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

package aerospike

import (
	"errors"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ShouldThrottle determines if we should throttle connection.
func ShouldThrottle(err error) bool {
	if err == nil {
		return false
	}

	var ae a.Error
	if errors.As(err, &ae) {
		return ae.Matches(types.NO_AVAILABLE_CONNECTIONS_TO_NODE) || ae.Matches(types.FAIL_FORBIDDEN)
	}

	// Addititonal errors that should be throttled.
	return errors.Is(err, a.ErrConnectionPoolEmpty) ||
		errors.Is(err, a.ErrConnectionPoolExhausted) ||
		errors.Is(err, a.ErrTooManyConnectionsForNode)
}
