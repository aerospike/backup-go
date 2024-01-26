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
	firstLT        = "first"
)

// literal asb tokens
const (
	namespaceToken  = "namespace"
	firstFileToken  = "first-file"
	asbVersionToken = "Version"
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

// escape character
const (
	asbEscape = '\\'
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
	Namespace string
	First     bool
}

type ASBReader struct {
	countingByteScanner
	parseErrArgs
	header   *header
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
		case globalSectionMarker:
			return r.readGlobals()
		case recordHeaderMarker:
			return r.readRecord()
		default:
			return nil, fmt.Errorf("read invalid line start character %c", b)
		}
	}()

	if err != nil {
		return nil, r.newParseError(err)
	}

	return v, nil
}

type header struct {
	Version string
}

func (r *ASBReader) readHeader() (*header, error) {

	r.section = headerS
	r.lineType = versionLT

	if err := _expectToken(r, asbVersionToken); err != nil {
		return nil, err
	}

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	// version number format is "x.y"
	ver, err := _readNBytes(r, 3)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return &header{
		Version: string(ver),
	}, nil
}

// readMetadata consumes all metadata lines
func (r *ASBReader) readMetadata() (*metaData, error) {
	var res metaData

	r.section = metadataS

	for {
		startC, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		// the metadata section is optional
		if startC != metadataSectionMarker {
			r.UnreadByte()
			break
		}

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		metaToken, err := _readUntilAny(r, []byte{' ', '\n'}, false)
		if err != nil {
			return nil, err
		}

		switch string(metaToken) {
		case namespaceToken:
			val, err := r.readNamespace()
			if err != nil {
				return nil, err
			}
			res.Namespace = val

		case firstFileToken:
			val, err := r.readFirst()
			if err != nil {
				return nil, err
			}
			res.First = val

		default:
			return nil, fmt.Errorf("unknown meta data line type %s", metaToken)
		}
	}

	return &res, nil
}

func (r *ASBReader) readNamespace() (string, error) {
	r.lineType = namespaceLT

	if err := _expectChar(r, ' '); err != nil {
		return "", err
	}

	bytes, err := _readUntil(r, '\n', true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (r *ASBReader) readFirst() (bool, error) {
	r.lineType = firstLT

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
		return nil, fmt.Errorf("invalid global line type %c", b)
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

// readSindex is used to read secondary index lines in the global section of the asb file.
// readSindex expects that r has been advanced past the secondary index global line markter '* i'
func (r *ASBReader) readSIndex() (*SecondaryIndex, error) {
	var res SecondaryIndex

	r.lineType = sindexLT

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	namespace, err := _readUntil(r, ' ', true)
	if err != nil {
		return nil, err
	}
	res.Namespace = string(namespace)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	set, err := _readUntil(r, ' ', true)
	if err != nil {
		return nil, err
	}
	res.Set = string(set)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	name, err := _readUntil(r, ' ', true)
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
		return nil, fmt.Errorf("invalid secondary index type %c", b)
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

		name, err := _readUntil(r, ' ', true)
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
			return nil, fmt.Errorf("invalid sindex path type %c", b)
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
		return nil, fmt.Errorf("invalid UDF type %c in global section UDF line", b)
	}

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	name, err := _readUntil(r, ' ', true)
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
type userKey struct {
	Ktype KeyType
	Value any
}

type recordData struct {
	key        userKey // optional
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
			return nil, fmt.Errorf("invalid record header line type %c expected %c", b, expectedRecordHeaderTypes[i])
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
			namespace, err := _readUntil(r, '\n', true)
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
	'N': {}, // nil
	'Z': {}, // bool
	'I': {}, // int64
	'D': {}, // float64
	'S': {}, // string
	// bytes types
	'B': {}, // bytes
	'J': {}, // java bytes
	'C': {}, // c# bytes
	'P': {}, // python bytes
	'R': {}, // ruby bytes
	'H': {}, // php bytes
	'E': {}, // erlang bytes
	'Y': {}, // HLL bytes
	'M': {}, // map bytes
	'L': {}, // list bytes
	// end bytes types
	'U': {}, // LDT
	'X': {}, // base64 encoded string
	'G': {}, // geojson
}

func (r *ASBReader) readBin(bins a.BinMap) error {
	r.lineType = binLT

	binType, err := r.ReadByte()
	if err != nil {
		return err
	}

	if _, ok := binTypes[binType]; !ok {
		return fmt.Errorf("invalid bin type %c", binType)
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
		return fmt.Errorf("invalid character in bytes bin %c, expected '!' or ' '", b)
	}

	if !base64Encoded {
		if err := _expectChar(r, ' '); err != nil {
			return err
		}
	}

	nameBytes, err := _readUntilAny(r, []byte{' ', '\n'}, true)
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
		if base64Encoded {
			binVal, binErr = _readBase64Bytes(r, ' ')
		} else {
			binVal, binErr = _readBytes(r, ' ')
		}
	}

	if binErr != nil {
		return binErr
	}

	if err := _expectChar(r, '\n'); err != nil {
		return err
	}

	bins[name] = binVal

	return nil
}

var asbKeyTypes = map[byte]struct{}{
	'I': {}, // int64
	'D': {}, // float64
	'S': {}, // string
	'X': {}, // base64 encoded string
	'B': {}, // bytes
}

// readKey reads a record key line from the asb file
// it expects that r has been advanced past the record key line marker '+ k'
func (r *ASBReader) readKey() (*userKey, error) {
	var res userKey

	r.lineType = keyLT

	keyTypeChar, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if _, ok := asbKeyTypes[keyTypeChar]; !ok {
		return nil, fmt.Errorf("invalid key type %c", keyTypeChar)
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// handle the special case where a bytes key is not base64 encoded
	var base64Encoded bool
	switch b {
	case '!':
	case ' ':
		base64Encoded = true
	default:
		return nil, fmt.Errorf("invalid character %c, expected '!' or ' '", keyTypeChar)
	}

	if !base64Encoded {
		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}
	}

	switch keyTypeChar {
	case 'I':
		res.Ktype = KeyTypeInteger
		keyVal, err := _readInteger(r, '\n')
		if err != nil {
			return nil, err
		}
		res.Value = keyVal

	case 'D':
		res.Ktype = KeyTypeFloat
		keyVal, err := _readFloat(r, '\n')
		if err != nil {
			return nil, err
		}
		res.Value = keyVal

	case 'S':
		res.Ktype = KeyTypeString

		keyVal, err := _readString(r, ' ')
		if err != nil {
			return nil, err
		}

		res.Value = keyVal

	case 'X':
		res.Ktype = KeyTypeString

		keyVal, err := _readBase64Bytes(r, ' ')
		if err != nil {
			return nil, err
		}

		res.Value = string(keyVal)

	case 'B':
		res.Ktype = KeyTypeBlob

		var keyVal []byte
		if base64Encoded {
			keyVal, err = _readBase64Bytes(r, ' ')
		} else {
			keyVal, err = _readBytes(r, ' ')
		}

		if err != nil {
			return nil, err
		}

		res.Value = keyVal

	default:
		// should never happen because of the previous check for membership in asbKeyTypes
		return nil, fmt.Errorf("invalid key type %c", keyTypeChar)
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

	set, err := _readUntil(r, '\n', true)
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

	// TODO make this a constant
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

	if err := _expectChar(src, sizeDelim); err != nil {
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
	bw, err := base64.StdEncoding.Decode(decoded, bytes)
	if err != nil {
		return nil, err
	}

	return decoded[:bw], nil
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

	if err := _expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	val, err := _readNBytes(src, int(length))
	if err != nil {
		return nil, err
	}

	return val, nil
}

func _readBool(src io.ByteScanner, delim byte) (bool, error) {
	bytes, err := _readUntil(src, delim, false)
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
		return false, fmt.Errorf("invalid boolean character %c", b)
	}
}

func _readFloat(src io.ByteScanner, delim byte) (float64, error) {
	bytes, err := _readUntil(src, delim, false)
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
	bytes, err := _readUntil(src, delim, false)
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
	bytes, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(string(bytes), 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(num), nil
}

func _readUntil(src io.ByteScanner, delim byte, escaped bool) ([]byte, error) {
	return _readUntilAny(src, []byte{delim}, escaped)
}

func _readUntilAny(src io.ByteScanner, delims []byte, escaped bool) ([]byte, error) {
	var (
		bts []byte
		esc bool
		i   int
	)

	for i = 0; i < maxTokenSize; i++ {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		// read past the escape character
		if escaped && b == asbEscape && !esc {
			esc = true
			continue
		}

		if !esc && bytes.ContainsRune(delims, rune(b)) {
			// hit a delimiter so return
			return bts, src.UnreadByte()
		}

		esc = false

		bts = append(bts, b)

		if i == maxTokenSize-1 {
			return nil, fmt.Errorf("token larger than max size")
		}
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
	return _expectAnyChar(src, []byte{c})
}

func _expectAnyChar(src io.ByteReader, chars []byte) error {
	b, err := src.ReadByte()
	if err != nil {
		return err
	}

	if !bytes.ContainsRune(chars, rune(b)) {
		return fmt.Errorf("invalid character, read %c, expected one of %s", b, string(chars))
	}

	return nil
}

func _expectToken(src io.ByteReader, token string) error {
	bytes, err := _readNBytes(src, len(token))
	if err != nil {
		return err
	}

	if string(bytes) != token {
		return fmt.Errorf("invalid token, read %s, expected %s", string(bytes), token)
	}

	return nil
}
