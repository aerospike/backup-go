package unmarshal

// TODO make this a unique type
type SIPathBinType = byte

const (
	NumericSIDataType     SIPathBinType = 'N'
	StringSIDataType      SIPathBinType = 'S'
	GEO2DSphereSIDataType SIPathBinType = 'G'
	BlobSIDataType        SIPathBinType = 'B'
)

type SIndexType byte

const (
	BinSIndex         SIndexType = 'N'
	ListElementSIndex SIndexType = 'L'
	MapKeySIndex      SIndexType = 'K'
	MapValueSIndex    SIndexType = 'V'
)

type SIndexPath struct {
	BinName string
	BinType SIPathBinType
}

type SecondaryIndex struct {
	Namespace     string
	Set           string
	Name          string
	IndexType     SIndexType
	Paths         []*SIndexPath
	ValuesCovered int
}
