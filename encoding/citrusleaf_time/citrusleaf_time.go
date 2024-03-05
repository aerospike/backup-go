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
