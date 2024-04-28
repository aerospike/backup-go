package models

import (
	"time"
)

// TimeBounds represents a period of time between two timestamps.
type TimeBounds struct {
	FromTime *time.Time
	ToTime   *time.Time
}
