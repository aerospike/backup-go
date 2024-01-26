package unmarshal

type UDFType = byte

const (
	LUAUDFType UDFType = 'L'
)

type UDF struct {
	UDFType UDFType
	Name    string
	Content []byte
}
