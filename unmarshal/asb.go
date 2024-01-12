package unmarshal

import (
	"fmt"
	"io"
	"strconv"
)

type section int

// sections
const (
	headerS   = "header"
	metadataS = "meta-data"
	globalS   = "global"
)

// line types
const (
	versionLT   = "version"
	namespaceLT = "namespace"
	UDFLT       = "UDF"
	sindexLT    = "sindex"
)

const (
	namespaceToken      = "namespace"
	firstFileToken      = "first-file"
	globalSectionMarker = "*"
	maxNamespaceLength  = 31
	maxTokenSize        = 1000
)

type CountingByteScanner struct {
	io.ByteScanner
	count uint64
}

func (c *CountingByteScanner) ReadByte() (byte, error) {
	b, err := c.ByteScanner.ReadByte()
	if err != nil {
		return 0, err
	}

	c.count++

	return b, err
}

func (c *CountingByteScanner) UnreadByte() error {
	err := c.ByteScanner.UnreadByte()
	if err != nil {
		return err
	}

	c.count--

	return err
}

type ParseErrArgs struct {
	section  string
	lineType string
}

type ASBReader struct {
	CountingByteScanner
	ParseErrArgs
	hasReadHeader bool
}

func (r *ASBReader) newParseError(err error) error {
	return fmt.Errorf("parsing failed for section: %s, type: %s, at character: %d error: %w", r.section, r.lineType, r.count, err)
}

// TODO wrap errors returned from this with a character count
func (r *ASBReader) NextToken() (any, error) {

	v, err := func() (any, error) {
		if !r.hasReadHeader {
			return r.readHeader()
			r.hasReadHeader = true
		}

		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		switch b {
		case '#':
			return r.readMetadata()
		case '*':
			return r.readGlobals()

		default:
			return nil, fmt.Errorf("read invalid asb line start character %s", b)
		}
	}()

	if err != nil {
		return nil, r.newParseError(err)
	}

	return v, nil
}

type Header struct {
	version string
}

func (r *ASBReader) readHeader() (*Header, error) {

	r.section = headerS
	r.lineType = versionLT

	versionTextLen := len("Version x.y\n")
	bytes := make([]byte, versionTextLen)
	for i := 0; i < versionTextLen; i++ {
		var err error
		bytes[i], err = r.ReadByte()
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
// TODO consume the start character '#' here
func (r *ASBReader) readMetadata() (*MetaData, error) {
	var res MetaData

	r.section = metadataS

	namespace, err := r.readNamespace()
	if err != nil {
		return nil, err
	}
	res.namespace = namespace

	first, err := r.readFirst()
	if err != nil {
		return nil, err
	}
	res.first = first

	return &res, nil
}

func (r *ASBReader) readNamespace() (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	if b != '#' {
		return "", fmt.Errorf("invalid character %b in metadata section namespace line", b)
	}

	if err := _expectChar(r, ' '); err != nil {
		return "", err
	}

	if err := _expectToken(r, namespaceToken); err != nil {
		return "", err
	}

	if err := _expectChar(r, ' '); err != nil {
		return "", err
	}

	//TODO support escaped namespaces that might have \n in them
	bytes, err := _readUntil(r, '\n')
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (r *ASBReader) readFirst() (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}

	// The first-file metadata line is optional
	if b != '#' {
		return false, nil
	}

	if err := _expectChar(r, ' '); err != nil {
		return false, err
	}

	if err := _expectToken(r, firstFileToken); err != nil {
		return false, nil
	}

	if err := _expectChar(r, '\n'); err != nil {
		return false, err
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

type SIDataType = byte

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

type Globals struct {
	secondaryIndexes []*SecondaryIndex
	UDFs             []*UDF
}

func (r *ASBReader) readGlobals() (*Globals, error) {
	var res Globals

	r.section = globalS

	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		if b != '*' {
			break
		}

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		b, err = r.ReadByte()
		if err != nil {
			return nil, err
		}

		var udfCount int
		var sindexCount int

		switch b {
		case 'i':
		case 'u':
			udf, err := r.readUDF()
			if err != nil {
				return nil, err
			}
			res.UDFs[udfCount] = udf

		default:
			return nil, fmt.Errorf("invalid global line type %b", b)
		}
	}

	return &res, nil
}

// NOTE this is meant to read the UDF line AFTER the global start '* [SP] i'
func (r *ASBReader) readSIndex() (*SecondaryIndex, error) {
	var res SecondaryIndex

	r.lineType = sindexLT

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

}

// NOTE this is meant to read the UDF line AFTER the global start '* [SP] u'
func (r *ASBReader) readUDF() (*UDF, error) {
	var res UDF

	r.lineType = UDFLT

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case LUAUDFType:
		res.udfType = LUAUDFType
	default:
		return nil, fmt.Errorf("invalid UDF type %b in global section UDF line", b)
	}

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	name, err := _readUntil(r, ' ')
	if err != nil {
		return nil, err
	}
	res.name = string(name)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	length, err := _readSize(r, ' ')
	if err != nil {
		return nil, err
	}
	res.length = length

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	content, err := _readNBytes(r, int(length))
	if err != nil {
		return nil, err
	}
	res.content = content

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return &res, nil

}

// ***** Helper Functions

// _readSize reads a size or length token from the asb format
// the value should fit in a uint32
func _readSize(src io.ByteScanner, delim byte) (uint32, error) {
	bytes, err := _readUntil(src, delim)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(string(bytes), 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(num), nil
}

func _readUntil(src io.ByteScanner, delim byte) ([]byte, error) {
	bytes := make([]byte, maxTokenSize)
	var i int
	for i = 0; i < maxTokenSize; i++ {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		if b == delim {
			err := src.UnreadByte()
			if err != nil {
				return nil, err
			}
		}

		bytes[i] = b
	}

	return bytes, nil
}

func _readNBytes(src io.ByteReader, n int) ([]byte, error) {
	bytes := make([]byte, n)
	var i int
	for i = 0; i < n; i++ {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		bytes[i] = b
	}

	return bytes, nil
}

func _expectChar(src io.ByteReader, c byte) error {
	b, err := src.ReadByte()
	if err != nil {
		return err
	}

	if b != c {
		return fmt.Errorf("invalid character, read %b, wanted %b", b, c)
	}

	return nil
}

func _expectToken(src io.ByteReader, token string) error {
	bytes, err := _readNBytes(src, len(token))
	if err != nil {
		return err
	}

	if string(bytes) != firstFileToken {
		return fmt.Errorf("invalid token, read %s, wanted %s", string(bytes), firstFileToken)
	}

	return nil
}
