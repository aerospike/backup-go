// Copyright 2024-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decoder

import (
	"backuplib/models"
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	a "github.com/aerospike/aerospike-client-go/v7"
)

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
	maxGeneration      = math.MaxUint16
	maxBinCount        = math.MaxUint16
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

// TODO maybe use error functions for each asb level, reader, section, line type
// that way you don't have to track section and line type in the reader
// this would allow passing an interface for the reader to most functions
// and would allow the reader to be used in a more generic way making tests easier

func newReaderError(offset uint64, err error) error {
	if err == io.EOF {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("error while reading asb file at character, %d: %w", offset, err)
}

func newSectionError(section string, err error) error {
	if err == io.EOF {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("error while reading section: %s, %w", section, err)
}

func newLineError(lineType string, err error) error {
	if err == io.EOF {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("error while reading line type: %s, %w", lineType, err)
}

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

type metaData struct {
	Namespace string
	First     bool
}

type ASBDecoder struct {
	countingByteScanner
	header   *header
	metaData *metaData
}

func NewASBDecoder(src io.Reader) (*ASBDecoder, error) {
	cbs := bufio.NewReader(src)
	asb := ASBDecoder{
		countingByteScanner: countingByteScanner{
			cbs,
			0,
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

func (r *ASBDecoder) NextToken() (any, error) {
	v, err := func() (any, error) {
		b, err := _peek(r)
		if err != nil {
			return nil, err
		}

		var v any

		switch b {
		case globalSectionMarker:
			v, err = r.readGlobals()
			err = newSectionError(globalS, err)
		case recordHeaderMarker:
			v, err = r.readRecord()
			err = newSectionError(recordS, err)
		default:
			v, err = nil, fmt.Errorf("read invalid line start character %c", b)
		}

		return v, err
	}()

	if err != nil {
		return nil, newReaderError(r.count, err)
	}

	return v, nil
}

type header struct {
	Version string
}

func (r *ASBDecoder) readHeader() (*header, error) {
	var res header

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

	res.Version = string(ver)

	return &res, nil
}

// readMetadata consumes all metadata lines
func (r *ASBDecoder) readMetadata() (*metaData, error) {
	var res metaData

	for {
		startC, err := _peek(r)
		if err != nil {
			return nil, err
		}

		// the metadata section is optional
		if startC != metadataSectionMarker {
			break
		}

		if err := _expectChar(r, metadataSectionMarker); err != nil {
			return nil, err
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
			if err := _expectChar(r, ' '); err != nil {
				return nil, err
			}

			val, err := r.readNamespace()
			if err != nil {
				return nil, newLineError(namespaceLT, err)
			}
			res.Namespace = val

		case firstFileToken:
			val, err := r.readFirst()
			if err != nil {
				return nil, newLineError(firstLT, err)
			}
			res.First = val

		default:
			return nil, fmt.Errorf("unknown meta data line type %s", metaToken)
		}
	}

	return &res, nil
}

func (r *ASBDecoder) readNamespace() (string, error) {
	bytes, err := _readUntil(r, '\n', true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (r *ASBDecoder) readFirst() (bool, error) {
	if err := _expectChar(r, '\n'); err != nil {
		return false, err
	}

	return true, nil

}

func (r *ASBDecoder) readGlobals() (any, error) {
	var res any

	if err := _expectChar(r, globalSectionMarker); err != nil {
		return nil, err
	}

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
		if err != nil {
			return nil, newLineError(sindexLT, err)
		}
	case 'u':
		res, err = r.readUDF()
		if err != nil {
			return nil, newLineError(UDFLT, err)
		}
	default:
		return nil, fmt.Errorf("invalid global line type %c", b)
	}

	return res, nil
}

// readSindex is used to read secondary index lines in the global section of the asb file.
// readSindex expects that r has been advanced past the secondary index global line markter '* i'
func (r *ASBDecoder) readSIndex() (*models.SIndex, error) {
	var res models.SIndex

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
		res.IndexType = models.BinSIndex
	case 'L':
		res.IndexType = models.ListElementSIndex
	case 'K':
		res.IndexType = models.MapKeySIndex
	case 'V':
		res.IndexType = models.MapValueSIndex
	default:
		return nil, fmt.Errorf("invalid secondary index type %c", b)
	}

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	// NOTE: the number of paths is always 1 for now
	// this means we read the value but don't use it
	npaths, err := _readSize(r, ' ')
	if err != nil {
		return nil, err
	}
	if npaths == 0 {
		return nil, errors.New("missing path(s) in sindex block")
	}

	var path models.SIndexPath

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	binName, err := _readUntil(r, ' ', true)
	if err != nil {
		return nil, err
	}
	path.BinName = string(binName)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	b, err = r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case 'S':
		path.BinType = models.StringSIDataType
	case 'N':
		path.BinType = models.NumericSIDataType
	case 'G':
		path.BinType = models.GEO2DSphereSIDataType
	case 'B':
		path.BinType = models.BlobSIDataType
	default:
		return nil, fmt.Errorf("invalid sindex path type %c", b)
	}

	// check for optional context
	b, err = _peek(r)
	if err != nil {
		return nil, err
	}

	if b == ' ' {
		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		// NOTE: the context should always be base64 encoded
		// so escaping is not needed
		context, err := _readUntil(r, '\n', false)
		if err != nil {
			return nil, err
		}

		path.B64Context = string(context)
	}

	res.Path = path

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return &res, nil
}

// readUDF is used to read UDF lines in the global section of the asb file.
// readUDF expects that r has been advanced past the UDF global line marker '* u '
func (r *ASBDecoder) readUDF() (*models.UDF, error) {
	var res models.UDF

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch models.UDFType(b) {
	case models.LUAUDFType:
		res.UDFType = models.LUAUDFType
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
	res.Name = string(name)

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	length, err := _readSize(r, ' ')
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	content, err := _readNBytes(r, int(length))
	if err != nil {
		return nil, err
	}
	res.Content = content

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

type recordData struct {
	userKey    any // optional
	namespace  string
	digest     []byte
	set        string // optional
	generation uint32
	expiration uint32
	binCount   uint16
}

var expectedRecordHeaderTypes = []byte{'k', 'n', 'd', 's', 'g', 't', 'b'}

func (r *ASBDecoder) readRecord() (*models.Record, error) {
	var recData recordData

	for i := 0; i < len(expectedRecordHeaderTypes); i++ {

		if err := _expectChar(r, recordHeaderMarker); err != nil {
			return nil, err
		}

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
			key, err := r.readUserKey()
			if err != nil {
				return nil, newLineError(keyLT, err)
			}
			recData.userKey = key

		case 1:
			namespace, err := r.readNamespace()
			if err != nil {
				return nil, newLineError(namespaceLT, err)
			}

			recData.namespace = string(namespace)

		case 2:
			digest, err := r.readDigest()
			if err != nil {
				return nil, newLineError(digestLT, err)
			}
			recData.digest = digest

		case 3:
			set, err := r.readSet()
			if err != nil {
				return nil, newLineError(setLT, err)
			}
			recData.set = set

		case 4:
			gen, err := r.readGeneration()
			if err != nil {
				return nil, newLineError(genLT, err)
			}
			recData.generation = gen

		case 5:
			exp, err := r.readExpiration()
			if err != nil {
				return nil, newLineError(expirationLT, err)
			}
			recData.expiration = exp

		case 6:
			binCount, err := r.readBinCount()
			if err != nil {
				return nil, newLineError(binCountLT, err)
			}
			recData.binCount = binCount

		default:
			// should never happen because this is set to the length of expectedRecordHeaderTypes
			return nil, fmt.Errorf("read too many record header lines, count: %d", i)
		}
	}

	var rec models.Record

	bins, err := r.readBins(recData.binCount)
	if err != nil {
		return nil, newLineError(recordBinsLT, err)
	}
	rec.Bins = bins

	key, err := a.NewKeyWithDigest(
		recData.namespace,
		recData.set,
		recData.userKey,
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

func (r *ASBDecoder) readBins(count uint16) (a.BinMap, error) {
	bins := make(a.BinMap, count)

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

// these are types that are stored as msgPack encoded bytes
var bytesToType = map[byte]struct{}{
	'Y': {},
	'M': {},
	'L': {},
}

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
	// bytes but parsed as another type
	'Y': {}, // HLL bytes
	'M': {}, // map bytes
	'L': {}, // list bytes
	// end bytes types
	'U': {}, // LDT
	'X': {}, // base64 encoded string
	'G': {}, // geojson
}

func (r *ASBDecoder) readBin(bins a.BinMap) error {
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
		binVal, binErr = _readBool(r)
	case 'I':
		binVal, binErr = _readInteger(r, '\n')
	case 'D':
		binVal, binErr = _readFloat(r, '\n')
	case 'S':
		binVal, binErr = _readStringSized(r, ' ')
	case 'U':
		return errors.New("this backup contains LDTs, please restore it using an older restore tool that supports LDTs")
	case 'X':
		val, err := _readBase64BytesSized(r, ' ')
		if err != nil {
			return err
		}

		binVal = string(val)
	case 'G':
		binVal, binErr = _readGeoJSON(r, ' ')
	}

	if _, ok := bytesBinTypes[binType]; ok {
		var val []byte

		if base64Encoded {
			val, binErr = _readBase64BytesSized(r, ' ')
		} else {
			val, binErr = _readBytesSized(r, ' ')
		}

		// bytes special cases
		if _, ok := bytesToType[binType]; ok {
			switch binType {
			case 'Y':
				// HLLs are treated as bytes by the client so no decode is needed
				binVal = a.NewHLLValue(val)
			case 'M':
				// maps need to be decoded so that the go client will write them as maps and not blobs
				return errors.New("map bins are not supported yet")
				// var m map[any]any
				// binErr = msgpack.Unmarshal(val, &m)
				// binVal = m
			case 'L':
				// lists are decoded for the same reason as maps
				return errors.New("list bins are not supported yet")
				// var l []any
				// binErr = msgpack.Unmarshal(val, &l)
				// binVal = l
			default:
				return fmt.Errorf("invalid bytes to type bin type %d", binType)
			}
		} else {
			binVal = val
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

// readUserKey reads a record key line from the asb file
// it expects that r has been advanced past the record key line marker '+ k'
func (r *ASBDecoder) readUserKey() (any, error) {
	var res any

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
		keyVal, err := _readInteger(r, '\n')
		if err != nil {
			return nil, err
		}
		res = keyVal

	case 'D':
		keyVal, err := _readFloat(r, '\n')
		if err != nil {
			return nil, err
		}
		res = keyVal

	case 'S':
		keyVal, err := _readStringSized(r, ' ')
		if err != nil {
			return nil, err
		}

		res = keyVal

	case 'X':
		keyVal, err := _readBase64BytesSized(r, ' ')
		if err != nil {
			return nil, err
		}

		res = string(keyVal)

	case 'B':
		var keyVal []byte
		if base64Encoded {
			keyVal, err = _readBase64BytesSized(r, ' ')
		} else {
			keyVal, err = _readBytesSized(r, ' ')
		}

		if err != nil {
			return nil, err
		}

		res = keyVal

	default:
		// should never happen because of the previous check for membership in asbKeyTypes
		return nil, fmt.Errorf("invalid key type %c", keyTypeChar)
	}

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return res, nil
}

func (r *ASBDecoder) readBinCount() (uint16, error) {
	binCount, err := _readInteger(r, '\n')
	if err != nil {
		return 0, err
	}

	if binCount > maxBinCount || binCount < 0 {
		return 0, fmt.Errorf("invalid bin count %d", binCount)
	}

	if err := _expectChar(r, '\n'); err != nil {
		return 0, err
	}

	return uint16(binCount), nil

}

// readExpiration reads an expiration line from the asb file
// it expects that r has been advanced past the expiration line marker '+ t '
// NOTE: we don't check the expiration against any bounds because negative (large) expirations are valid
// TODO expiration needs to be updated based on how much time has passed since the backup.
// I think that should be done in a processor though, not here
func (r *ASBDecoder) readExpiration() (uint32, error) {
	exp, err := _readInteger(r, '\n')
	if err != nil {
		return 0, err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return 0, err
	}

	if exp == 0 {
		// 0 is a special value that means never expire
		// the asb format stores this value as 0 but the server
		// needs (uint32)-1
		return math.MaxUint32, nil
	}

	if exp < 0 || exp > math.MaxUint32 {
		return 0, fmt.Errorf("invalid expiration time %d", exp)
	}

	return uint32(exp), nil
}

func (r *ASBDecoder) readGeneration() (uint32, error) {
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

func (r *ASBDecoder) readSet() (string, error) {
	set, err := _readUntil(r, '\n', true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return "", err
	}

	return string(set), err
}

func (r *ASBDecoder) readDigest() ([]byte, error) {
	digest, err := _readBase64BytesDelimited(r, '\n')
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return nil, err
	}

	return digest, nil
}

// ***** Helper Functions

func _readBase64BytesDelimited(src io.ByteScanner, delim byte) ([]byte, error) {
	encoded, err := _readUntil(src, delim, false)
	if err != nil {
		return nil, err
	}

	return _decodeBase64(encoded)
}

func _readBase64BytesSized(src io.ByteScanner, sizeDelim byte) ([]byte, error) {
	size, err := _readSize(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	return _readBlockDecodeBase64(src, int(size))
}

func _readBlockDecodeBase64(src io.ByteReader, n int) ([]byte, error) {
	bytes, err := _readNBytes(src, n)
	if err != nil {
		return nil, err
	}

	return _decodeBase64(bytes)
}

func _decodeBase64(src []byte) ([]byte, error) {
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(src)))
	bw, err := base64.StdEncoding.Decode(decoded, src)
	if err != nil {
		return nil, err
	}

	return decoded[:bw], nil
}

func _readStringSized(src io.ByteScanner, sizeDelim byte) (string, error) {
	val, err := _readBytesSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	return string(val), err
}

func _readBytesSized(src io.ByteScanner, sizeDelim byte) ([]byte, error) {
	length, err := _readSize(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	return _readNBytes(src, int(length))
}

func _readBool(src io.ByteScanner) (bool, error) {
	b, err := src.ReadByte()
	if err != nil {
		return false, err
	}

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

	return strconv.ParseFloat(string(bytes), 64)
}

func _readInteger(src io.ByteScanner, delim byte) (int64, error) {
	bytes, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(bytes), 10, 64)
}

func _readGeoJSON(src io.ByteScanner, sizeDelim byte) (a.GeoJSONValue, error) {
	val, err := _readStringSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	return a.NewGeoJSONValue(val), nil
}

func _readHLL(src io.ByteScanner, sizeDelim byte) (a.HLLValue, error) {
	bytes, err := _readBytesSized(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	return a.NewHLLValue(bytes), nil
}

// _readSize reads a size or length token from the asb format
// the value should fit in a uint32
func _readSize(src io.ByteScanner, delim byte) (uint32, error) {
	bytes, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(string(bytes), 10, 32)

	return uint32(num), err
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
	}

	return nil, errors.New("token larger than max size")
}

func _readNBytes(src io.ByteReader, n int) ([]byte, error) {
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
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
		if len(chars) == 1 {
			return fmt.Errorf("invalid character, read %c, expected %c", b, chars[0])
		} else {
			return fmt.Errorf("invalid character, read %c, expected one of %s", b, string(chars))
		}
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

func _peek(src io.ByteScanner) (byte, error) {
	b, err := src.ReadByte()
	if err != nil {
		return 0, err
	}

	err = src.UnreadByte()

	return b, err
}
