package models

import a "github.com/aerospike/aerospike-client-go/v7"

type XDRMeta struct {
	Key        *a.Key
	Operation  *a.Operation
	Generation int
	// TODO: may be remove "Ms" from name?
	LastUpdateTimeMs   int64
	ExpiryTime         int
	RecordExistsAction *a.RecordExistsAction
	GenerationPolicy   *a.GenerationPolicy
}

type XDRRecord struct {
	Metadata XDRMeta
	Bins     map[string]any
}
