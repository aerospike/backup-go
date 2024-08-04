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

package cltime

import "time"

// getNow is a variable that holds the time.Now function. It is used for testing
var getNow = time.Now

// CitrusleafEpoch is the start time in seconds of the citrusleaf epoch
// relative to the Unix epoch.
const CitrusleafEpoch int64 = 1262304000

// CLTime is a struct that represents a time in seconds since the citrusleaf epoch.
type CLTime struct {
	Seconds int64
}

// NewCLTime returns a new CLTime object with the given number of seconds.
func NewCLTime(seconds int64) CLTime {
	return CLTime{Seconds: seconds}
}

// Unix returns the time in seconds since the Unix epoch.
func (c *CLTime) Unix() int64 {
	return c.Seconds + CitrusleafEpoch
}

// Now returns the current time in seconds since the citrusleaf epoch.
func Now() CLTime {
	return CLTime{Seconds: getNow().Unix() - CitrusleafEpoch}
}
