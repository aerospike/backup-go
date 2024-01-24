package unmarshal

type UDFType = byte

const (
	LUAUDFType UDFType = 'L'
)

type UDF struct {
	udfType UDFType
	name    string
	length  uint32
	content []byte
}
