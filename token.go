package backuplib

import (
	"github.com/aerospike/aerospike-tools-backup-lib/models"
)

type tokenType uint8

const (
	tokenTypeInvalid tokenType = iota
	tokenTypeRecord
	tokenTypeSIndex
	tokenTypeUDF
)

// token is the struct used by backup and restore pipelines to pass data between workers
// fields should be accessed based on the tokenType
type token struct {
	Record *models.Record
	SIndex *models.SIndex
	UDF    *models.UDF
	Type   tokenType
}

// newRecordToken creates a new token with the given record
func newRecordToken(r *models.Record) *token {
	return &token{
		Record: r,
		Type:   tokenTypeRecord,
	}
}

// newSIndexToken creates a new token with the given secondary index
func newSIndexToken(s *models.SIndex) *token {
	return &token{
		SIndex: s,
		Type:   tokenTypeSIndex,
	}
}

// newUDFToken creates a new token with the given UDF
func newUDFToken(u *models.UDF) *token {
	return &token{
		UDF:  u,
		Type: tokenTypeUDF,
	}
}
