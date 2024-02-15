package models

import (
	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Records ****

type Record = a.Record

// **** SIndexes ****

type SIPathBinType byte

const (
	InvalidSIDataType     SIPathBinType = 0
	NumericSIDataType     SIPathBinType = 'N'
	StringSIDataType      SIPathBinType = 'S'
	GEO2DSphereSIDataType SIPathBinType = 'G'
	BlobSIDataType        SIPathBinType = 'B'
)

type SIndexType byte

const (
	InvalidSIndex     SIndexType = 0
	BinSIndex         SIndexType = 'N'
	ListElementSIndex SIndexType = 'L'
	MapKeySIndex      SIndexType = 'K'
	MapValueSIndex    SIndexType = 'V'
)

type SIndexPath struct {
	BinName    string
	BinType    SIPathBinType
	B64Context string
}

type SIndex struct {
	Namespace string
	Set       string
	Name      string
	IndexType SIndexType
	Path      SIndexPath
}

// **** UDFs ****

type UDFType byte

const (
	LUAUDFType UDFType = 'L'
)

type UDF struct {
	UDFType UDFType
	Name    string
	Content []byte
}
