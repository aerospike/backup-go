package models

import "errors"

// ErrFilteredOut is returned by a Data Processor when a token
// should be filtered out of the pipeline.
var ErrFilteredOut = errors.New("filtered out")
