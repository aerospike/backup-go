package unmarshal

import (
	"fmt"
	"io"
)

type section int

const (
	invalidSection section = iota
	metadata
	global
	records
)

const (
	namespaceToken      = "namespace"
	firstFileToken      = "first-file"
	globalSectionMarker = "*"
	maxNamespaceLength  = 31
)

type CountingByteReader struct {
	io.ByteReader
	count uint64
}

func (c *CountingByteReader) ReadByte() (byte, error) {
	b, err := c.ByteReader.ReadByte()
	if err != nil {
		return 0, err
	}

	c.count++

	return b, err
}

type ASBReader struct {
	source        *CountingByteReader // TODO maybe replace this with an InputOpener that can open itself
	hasReadHeader bool
}

// TODO wrap errors returned from this with a character count
func (r *ASBReader) NextToken() (any, error) {
	if !r.hasReadHeader {
		return readHeader(r.source)
		r.hasReadHeader = true
	}

	b, err := r.source.ReadByte()
	if err != nil {
		err = fmt.Errorf("read failed at character %d %w", r.source.count, err)
		return nil, err
	}

	switch b {
	case '#':
		return readMetadata(r.source)
	case '*':
		return readGlobal(r.source)

	default:
		return nil, fmt.Errorf("read invalid asb line start character %s", b)
	}
}

type Header struct {
	version string
}

func readHeader(src io.ByteReader) (*Header, error) {
	versionTextLen := len("Version x.y\n")
	bytes := make([]byte, versionTextLen)
	for i := 0; i < versionTextLen; i++ {
		var err error
		bytes[i], err = src.ReadByte()
		if err != nil {
			return nil, err
		}
	}

	return &Header{
		version: string(bytes[7:10]),
	}, nil
}

type MetaData struct {
	namespace string
	first     bool
}

// TODO handle namespaces with escaped characters (e.g. space or line feed)
func readMetadata(src io.ByteReader) (*MetaData, error) {
	namespace, err := readNamespace(src)
	if err != nil {
		return nil, err
	}
}

func readNamespace(src io.ByteReader) (string, error) {
	b, err := src.ReadByte()
	if err != nil {
		return "", err
	}

	if b != '#' {
		return "", fmt.Errorf("invalid character %b in metadata section namespace line", b)
	}

	b, err = src.ReadByte()
	if err != nil {
		return "", err
	}

	if b != ' ' {
		return "", fmt.Errorf("invalid character %b in metadata section namespace line", b)
	}

	namespaceLen := len(namespaceToken)
	bytes := make([]byte, namespaceLen)
	for i := 0; i < namespaceLen; i++ {
		var err error
		bytes[i], err = src.ReadByte()
		if err != nil {
			return "", err
		}
	}

	namespace := string(bytes)

	if namespace != namespaceToken {
		return "", fmt.Errorf("invalid namespace token %s in metadata namespace line", namespaceToken)
	}

	b, err = src.ReadByte()
	if err != nil {
		return "", err
	}

	if b != ' ' {
		return "", fmt.Errorf("invalid character %b in metadata namespace line", b)
	}

	bytes = []byte{}
	var count int
	for {
		// TODO support escaped namespaces?

		b, err = src.ReadByte()
		if err != nil {
			return "", err
		}
		count++

		if b == byte('\n') {
			break
		}

		bytes[count] = b
	}

	return string(bytes), nil
}

func readFirst(src io.ByteReader) (bool, error) {
	b, err := src.ReadByte()
	if err != nil {
		return false, err
	}

	// The first-file metadata line is optional
	if b != '#' {
		return false, nil
	}

	b, err = src.ReadByte()
	if err != nil {
		return false, err
	}

	if b != ' ' {
		return false, fmt.Errorf("invalid character %b in metadata section first-file line", b)
	}

	firstLen := len(firstFileToken)
	bytes := make([]byte, firstLen)
	for i := 0; i < firstLen; i++ {
		var err error
		bytes[i], err = src.ReadByte()
		if err != nil {
			return false, err
		}
	}

	first := string(bytes)

	if first != firstFileToken {
		return false, fmt.Errorf("invalid first-file token %s in metadata section first-file line", firstFileToken)
	}

	b, err = src.ReadByte()
	if err != nil {
		return false, err
	}

	if b != '\n' {
		return false, fmt.Errorf("invalid character %b in metadata section first-file line", b)
	}

	return true, nil

}

type SIndexType byte

const (
	BinIndex         SIndexType = 'N'
	ListElementIndex SIndexType = 'L'
	MapKeyIndex      SIndexType = 'K'
	MapValueIndex    SIndexType = 'V'
)

type SIDataType byte

const (
	InvalidSIDataType     SIDataType = 'I'
	NumericSIDataType     SIDataType = 'N'
	StringSIDataType      SIDataType = 'S'
	GEO2DSphereSIDataType SIDataType = 'G'
	BlobSIDataType        SIDataType = 'B'
)

type SecondaryIndex struct {
	namespace     string
	set           string
	name          string
	indexType     SIndexType
	path          string
	dataType      SIDataType
	valuesCovered int
}

type UDFType byte

const (
	LUAUDFType UDFType = 'L'
)

type UDF struct {
	udfType UDFType
	name    string
	length  uint32
	content []byte
}

type Globals struct {
	secondaryIndexes []SecondaryIndex
	UDFs             []UDF
}

func readGlobals(src io.ByteReader) (*Globals, error) {
	for {

	}
}

// NOTE this is meant to read the UDF line AFTER the global start char *
func readUDFs(src io.ByteReader) (*UDF, error) {
	b, err := src.ReadByte()
	if err != nil {
		return nil, err
	}

	if b != ' ' {
		return nil, fmt.Errorf("invalid character %b in global section UDF line", b)
	}
}
