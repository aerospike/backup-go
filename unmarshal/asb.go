package unmarshal

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type section int

// section names
const (
	undefinedS = ""
	headerS    = "header"
	metadataS  = "meta-data"
	globalS    = "global"
	recordS    = "records"
)

// section markers
const (
	globalSectionMarker   byte = '*'
	metadataSectionMarker byte = '#'
	recordHeaderMarker    byte = '+'
	recordBinsMarker      byte = '-'
)

// line names
const (
	undefinedLT    = ""
	versionLT      = "version"
	namespaceLT    = "namespace"
	UDFLT          = "UDF"
	sindexLT       = "sindex"
	recordHeaderLT = "record header"
	recordBinsLT   = "record bins"
	binLT          = "bin"
	keyLT          = "key"
	digestLT       = "digest"
	setLT          = "set"
	genLT          = "generation"
	expirationLT   = "expiration"
	binCountLT     = "bin count"
)

// literal asb tokens
const (
	namespaceToken = "namespace"
	firstFileToken = "first-file"
)

// value bounds
const (
	maxNamespaceLength = 31
	maxTokenSize       = 1000
	maxGeneration      = 65535
	maxBinCount        = 65535
)

// asb boolean encoding
const (
	boolTrueByte  = 'T'
	boolFalseByte = 'F'
)

type countingByteScanner struct {
	io.ByteScanner
	count uint64
}

func (c *countingByteScanner) ReadByte() (byte, error) {
	b, err := c.ByteScanner.ReadByte()
	if err != nil {
		return 0, err
	}

	c.count++

	return b, err
}

func (c *countingByteScanner) UnreadByte() error {
	err := c.ByteScanner.UnreadByte()
	if err != nil {
		return err
	}

	c.count--

	return err
}

type parseErrArgs struct {
	section  string
	lineType string
}

type metaData struct {
	namespace string
	first     bool
}

type ASBReader struct {
	countingByteScanner
	parseErrArgs
	header   *Header
	metaData *metaData
}

func NewASBReader(src io.Reader) (*ASBReader, error) {
	cbs := bufio.NewReader(src)
	asb := ASBReader{
		countingByteScanner: countingByteScanner{
			cbs,
			0,
		},
		parseErrArgs: parseErrArgs{
			section:  undefinedS,
			lineType: undefinedLT,
		},
	}

	header, err := asb.readHeader()
	if err != nil {
		return nil, fmt.Errorf("error while reading header: %w", err)
	}
	asb.header = header
	// TODO make sure file version is 3.1

	meta, err := asb.readMetadata()
	if err != nil {
		return nil, fmt.Errorf("error while reading metadata: %w", err)
	}
	asb.metaData = meta

	return &asb, nil

}

func (r *ASBReader) newParseError(err error) error {
	return fmt.Errorf("parsing failed for section: %s, type: %s, at character: %d error: %w", r.section, r.lineType, r.count, err)
}

// TODO wrap errors returned from this with a character count
func (r *ASBReader) NextToken() (any, error) {

	v, err := func() (any, error) {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		switch b {
		case metadataSectionMarker:
			return r.readMetadata()
		case globalSectionMarker:
			return r.readGlobals()
		case recordHeaderMarker:
			return r.readRecord()
		default:
			return nil, fmt.Errorf("read invalid line start character %b", b)
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
		version: string(bytes[8:11]),
	}, nil
}

// readMetadata consumes all metadata lines
func (r *ASBReader) readMetadata() (*metaData, error) {
	var res metaData

	r.section = metadataS

	namespace, err := r.readNamespace()
	if err != nil {
		return nil, err
	}
	res.namespace = namespace

	if err := _expectChar(r, metadataSectionMarker); err != nil {
		return nil, err
	}

	first, err := r.readFirst()
	if err != nil {
		return nil, err
	}
	res.first = first

	return &res, nil
}

func (r *ASBReader) readNamespace() (string, error) {
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

func (r *ASBReader) readGlobals() (any, error) {
	var res any

	r.section = globalS

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case 'i':
		res, err = r.readSIndex()
	case 'u':
		res, err = r.readUDF()
	default:
		return nil, fmt.Errorf("invalid global line type %b", b)
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

// NOTE this is meant to read the UDF line AFTER the global start '* [SP] i'
func (r *ASBReader) readSIndex() (*SecondaryIndex, error) {
	var res SecondaryIndex

	r.lineType = sindexLT

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	namespace, err := _readUntil(r, ' ')
	if err != nil {
		return nil, err
	}
	res.Namespace = string(namespace)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	set, err := _readUntil(r, ' ')
	if err != nil {
		return nil, err
	}
	res.Set = string(set)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	name, err := _readUntil(r, ' ')
	if err != nil {
		return nil, err
	}
	res.Name = string(name)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case 'N':
		res.IndexType = BinSIndex
	case 'L':
		res.IndexType = ListElementSIndex
	case 'K':
		res.IndexType = MapKeySIndex
	case 'V':
		res.IndexType = MapValueSIndex
	default:
		return nil, fmt.Errorf("invalid secondary index type %b", b)
	}

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	npaths, err := _readSize(r, ' ')
	if err != nil {
		return nil, err
	}
	if npaths == 0 {
		return nil, errors.New("missing path(s) in sindex block")
	}
	res.ValuesCovered = int(npaths)

	res.Paths = make([]*SIndexPath, npaths)

	for i := uint32(0); i < npaths; i++ {
		var path SIndexPath

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		name, err := _readUntil(r, ' ')
		if err != nil {
			return nil, err
		}
		path.BinName = string(name)

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		switch b {
		case 'S':
			path.BinType = StringSIDataType
		case 'N':
			path.BinType = NumericSIDataType
		case 'G':
			path.BinType = GEO2DSphereSIDataType
		case 'B':
			path.BinType = BlobSIDataType
		default:
			return nil, fmt.Errorf("invalid sindex path type %b", b)
		}

		res.Paths[i] = &path
	}

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return &res, nil
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

type KeyType = byte

const (
	KeyTypeInteger KeyType = 'I'
	KeyTypeFloat   KeyType = 'D'
	KeyTypeString  KeyType = 'S'
	KeyTypeBlob    KeyType = 'B'
)

// TODO maybe this should just be an empty interface
// it's type can be inferred later
type Key struct {
	ktype KeyType
	value any
}

type recordData struct {
	key        Key // optional
	namespace  string
	digest     []byte
	set        string // optional
	generation uint32
	expiration uint32
	binCount   uint16
}

var expectedRecordHeaderTypes = []byte{'k', 'n', 'd', 's', 'g', 't', 'b'}

func (r *ASBReader) readRecord() (*Record, error) {
	var recData recordData

	r.section = recordS

	for i := 0; i < len(expectedRecordHeaderTypes); i++ {

		r.lineType = recordS

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		// "+ k" and "+ s" lines are optional
		if i == 0 && b == expectedRecordHeaderTypes[1] {
			i++
		} else if i == 3 && b == expectedRecordHeaderTypes[4] {
			i++
		} else if b != expectedRecordHeaderTypes[i] {
			return nil, fmt.Errorf("invalid record header line type %b expected %b", b, expectedRecordHeaderTypes[i])
		}

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		switch i {
		case 0:
			key, err := r.readKey()
			if err != nil {
				return nil, err
			}
			recData.key = *key

		case 1:
			// TODO use a generic readNamespace readString function here
			r.lineType = namespaceLT
			namespace, err := _readUntil(r, '\n')
			if err != nil {
				return nil, err
			}

			if err := _expectChar(r, '\n'); err != nil {
				return nil, err
			}

			recData.namespace = string(namespace)

		case 2:
			digest, err := r.readDigest()
			if err != nil {
				return nil, err
			}
			recData.digest = digest

		case 3:
			set, err := r.readSet()
			if err != nil {
				return nil, err
			}
			recData.set = set

		case 4:
			gen, err := r.readGeneration()
			if err != nil {
				return nil, err
			}
			recData.generation = gen

		case 5:
			exp, err := r.readExpiration()
			if err != nil {
				return nil, err
			}
			recData.expiration = exp

		case 6:
			binCount, err := r.readBinCount()
			if err != nil {
				return nil, err
			}
			recData.binCount = binCount

		default:
			return nil, fmt.Errorf("read too many record header lines, count: %d", i)
		}

		if i < len(expectedRecordHeaderTypes) {
			if err := _expectChar(r, recordHeaderMarker); err != nil {
				return nil, err
			}
		}
	}

	var rec Record

	bins, err := r.readBins(recData.binCount)
	if err != nil {
		return nil, err
	}
	rec.Bins = bins

	key, err := a.NewKeyWithDigest(
		recData.namespace,
		recData.set,
		recData.key,
		recData.digest,
	)
	if err != nil {
		return nil, err
	}

	rec.Key = key
	rec.Bins = bins
	rec.Expiration = recData.expiration
	rec.Generation = recData.generation

	return &rec, nil
}

func (r *ASBReader) readBins(count uint16) (a.BinMap, error) {
	bins := make(a.BinMap, count)

	r.lineType = recordBinsLT

	for i := uint16(0); i < count; i++ {
		if err := _expectChar(r, recordBinsMarker); err != nil {
			return nil, err
		}

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		err := r.readBin(bins)
		if err != nil {
			return nil, err
		}

	}

	return bins, nil
}

var bytesBinTypes = map[byte]struct{}{
	'B': {},
	'J': {},
	'C': {},
	'P': {},
	'R': {},
	'H': {},
	'E': {},
	'Y': {},
	'M': {},
	'L': {},
}

// TODO make this use bytesBinTypes
var binTypes = map[byte]struct{}{
	// basic types
	'N': {},
	'Z': {},
	'I': {},
	'D': {},
	'S': {},
	// bytes types
	'B': {},
	'J': {},
	'C': {},
	'P': {},
	'R': {},
	'H': {},
	'E': {},
	'Y': {},
	'M': {},
	'L': {},
	// end bytes types
	'U': {},
	'X': {},
	'G': {},
}

func (r *ASBReader) readBin(bins a.BinMap) error {
	r.lineType = binLT

	binType, err := r.ReadByte()
	if err != nil {
		return err
	}

	if _, ok := binTypes[binType]; !ok {
		return fmt.Errorf("invalid bin type %b", binType)
	}

	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	var base64Encoded bool
	switch b {
	case '!':
	case ' ':
		base64Encoded = true
	default:
		return fmt.Errorf("invalid character in bytes bin %b, expected '!' or ' '", b)
	}

	if base64Encoded {
		if err := _expectChar(r, ' '); err != nil {
			return err
		}
	}

	nameBytes, err := _readUntilAny(r, []byte{' ', '\n'})
	if err != nil {
		return err
	}
	name := string(nameBytes)

	var binVal any
	var binErr error

	// 'N' is a special case where the line ends after the bin name
	if binType == 'N' {
		if err := _expectChar(r, '\n'); err != nil {
			return err
		}

		bins[name] = nil

		return nil
	} else {
		if err := _expectChar(r, ' '); err != nil {
			return err
		}
	}

	switch binType {
	case 'Z':
		binVal, binErr = _readBool(r, '\n')
	case 'I':
		binVal, binErr = _readInteger(r, '\n')
	case 'D':
		binVal, binErr = _readFloat(r, '\n')
	case 'S':
		binVal, binErr = _readString(r, ' ')
	case 'U':
		return errors.New("this backup contains LDTs, please restore it using an older restore tool that supports LDTs")
	case 'X':
		val, err := _readBase64Bytes(r, ' ')
		if err != nil {
			return err
		}

		binVal = string(val)
	case 'G':
		binVal, binErr = _readString(r, ' ')
	}

	if _, ok := bytesBinTypes[binType]; ok {
		binVal, binErr = _readBytes(r, ' ')
	}

	if binErr != nil {
		return binErr
	}

	bins[name] = binVal

	return nil
}

func (r *ASBReader) readKey() (*Key, error) {
	var res Key

	r.lineType = keyLT

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// TODO check that b is a valid key type here
	// so that the error will match the character count

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	switch b {
	case 'I':
		res.ktype = KeyTypeInteger
		keyVal, err := _readInteger(r, '\n')
		if err != nil {
			return nil, err
		}
		res.value = keyVal

	case 'D':
		res.ktype = KeyTypeFloat
		keyVal, err := _readFloat(r, '\n')
		if err != nil {
			return nil, err
		}
		res.value = keyVal

	case 'S':
		res.ktype = KeyTypeString

		keyVal, err := _readString(r, ' ')
		if err != nil {
			return nil, err
		}

		res.value = keyVal

	// TODO why is this needed? is it legacy? what asbackup option produces base64 encoded key strings?
	case 'X':
		res.ktype = KeyTypeString

		keyVal, err := _readBase64Bytes(r, ' ')
		if err != nil {
			return nil, err
		}

		res.value = string(keyVal)

	case 'B':
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		var base64Encoded bool
		switch b {
		case '!':
		case ' ':
			base64Encoded = true
		default:
			return nil, fmt.Errorf("invalid character in bytes key %b, expected '!' or ' '", b)
		}

		if base64Encoded {
			if err := _expectChar(r, ' '); err != nil {
				return nil, err
			}
		}

		data, err := _readNBytes(r, ' ')
		if err != nil {
			return nil, err
		}

		var keyVal []byte
		if base64Encoded {
			keyVal = []byte{}
			base64.StdEncoding.Decode(keyVal, data)
		} else {
			keyVal = data
		}

		res.value = keyVal

	default:
		return nil, fmt.Errorf("invalid key type %b", b)
	}

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return &res, nil
}

func (r *ASBReader) readBinCount() (uint16, error) {
	r.lineType = binCountLT

	binCount, err := _readInteger(r, '\n')
	if err != nil {
		return 0, err
	}

	if binCount > maxBinCount {
		return 0, fmt.Errorf("invalid bin count %d", binCount)
	}

	if err := _expectChar(r, '\n'); err != nil {
		return 0, err
	}

	return uint16(binCount), nil

}

func (r *ASBReader) readExpiration() (uint32, error) {
	r.lineType = expirationLT

	exp, err := _readInteger(r, '\n')
	if err != nil {
		return 0, err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return 0, err
	}

	return uint32(exp), nil

}

func (r *ASBReader) readGeneration() (uint32, error) {
	r.lineType = genLT

	gen, err := _readInteger(r, '\n')
	if err != nil {
		return 0, err
	}

	if gen < 0 || gen > maxGeneration {
		return 0, fmt.Errorf("invalid generation count %d", gen)
	}

	if err := _expectChar(r, '\n'); err != nil {
		return 0, err
	}

	return uint32(gen), nil
}

func (r *ASBReader) readSet() (string, error) {
	r.lineType = setLT

	set, err := _readUntil(r, '\n')
	if err != nil {
		return "", err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return "", err
	}

	return string(set), err
}

func (r *ASBReader) readDigest() ([]byte, error) {
	r.lineType = digestLT

	digestSize := 20
	digest, err := _readBlockDecode(r, digestSize)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return digest, nil
}

// ***** Helper Functions

func _readBase64Bytes(src io.ByteScanner, sizeDelim byte) ([]byte, error) {
	size, err := _readSize(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	val, err := _readBlockDecode(src, int(size))
	if err != nil {
		return nil, err
	}

	return val, nil
}

func _readBlockDecode(src io.ByteReader, n int) ([]byte, error) {
	bytes, err := _readNBytes(src, n)
	if err != nil {
		return nil, err
	}

	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(bytes)))
	_, err = base64.StdEncoding.Decode(decoded, bytes)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

func _readString(src io.ByteScanner, sizeDelim byte) (string, error) {
	val, err := _readBytes(src, sizeDelim)
	if err != nil {
		return "", nil
	}

	return string(val), err
}

func _readBytes(src io.ByteScanner, sizeDelim byte) ([]byte, error) {
	length, err := _readSize(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	val, err := _readNBytes(src, int(length))
	if err != nil {
		return nil, err
	}

	return val, nil
}

func _readBool(src io.ByteScanner, delim byte) (bool, error) {
	bytes, err := _readUntil(src, delim)
	if err != nil {
		return false, err
	}

	b := bytes[0]
	switch b {
	case boolTrueByte:
		return true, nil
	case boolFalseByte:
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean character %b", b)
	}
}

func _readFloat(src io.ByteScanner, delim byte) (float64, error) {
	bytes, err := _readUntil(src, delim)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func _readInteger(src io.ByteScanner, delim byte) (int64, error) {
	bytes, err := _readUntil(src, delim)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseInt(string(bytes), 10, 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

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
	return _readUntilAny(src, []byte{delim})
}

func _readUntilAny(src io.ByteScanner, delims []byte) ([]byte, error) {
	bts := make([]byte, maxTokenSize)
	var i int
	for i = 0; i < maxTokenSize; i++ {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		if bytes.ContainsRune(delims, rune(b)) {
			err := src.UnreadByte()
			if err != nil {
				return nil, err
			}
		}

		bts[i] = b
	}

	return bts, nil
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
