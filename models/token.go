package models

import a "github.com/aerospike/aerospike-client-go/v7"

type TokenConstraint interface {
	*Token | *ASBXToken
}

type TokenType uint8

const (
	TokenTypeInvalid TokenType = iota
	TokenTypeRecord
	TokenTypeSIndex
	TokenTypeUDF
)

// Token encompasses the other data models.
// The fields should be accessed based on the tokenType.
type Token struct {
	SIndex *SIndex
	UDF    *UDF
	Record *Record
	Type   TokenType
	Size   uint64
	// Filter represents serialized partition filter for page, that record belongs to.
	// Is used only on pagination read, to save reading states.
	Filter *PartitionFilterSerialized
}

// NewRecordToken creates a new token with the given record.
func NewRecordToken(r *Record, size uint64, filter *PartitionFilterSerialized) *Token {
	return &Token{
		Record: r,
		Type:   TokenTypeRecord,
		Size:   size,
		Filter: filter,
	}
}

// NewSIndexToken creates a new token with the given secondary index.
func NewSIndexToken(s *SIndex, size uint64) *Token {
	return &Token{
		SIndex: s,
		Type:   TokenTypeSIndex,
		Size:   size,
	}
}

// NewUDFToken creates a new token with the given UDF.
func NewUDFToken(u *UDF, size uint64) *Token {
	return &Token{
		UDF:  u,
		Type: TokenTypeUDF,
		Size: size,
	}
}

// ASBXToken represents data received from XDR or RAW payload data.
type ASBXToken struct {
	Key     *a.Key
	Payload []byte
}

// NewASBXToken creates new ASBX Token from XDR or RAW payload data.
func NewASBXToken(key *a.Key, payload []byte) *ASBXToken {
	return &ASBXToken{
		Key:     key,
		Payload: payload,
	}
}
