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

package asb

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"

	a "github.com/aerospike/aerospike-client-go/v7"
	particleType "github.com/aerospike/aerospike-client-go/v7/types/particle_type"
	"github.com/aerospike/backup-go/models"
)

func newDecoderError(offset uint64, err error) error {
	if errors.Is(err, io.EOF) {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("error while reading asb data at byte %d: %w", offset, err)
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

// Decoder is used to decode an asb file
type Decoder struct {
	header   *header
	metaData *metaData
	countingByteScanner
}

func NewDecoder(src io.Reader) (*Decoder, error) {
	cbs := bufio.NewReader(src)
	asb := Decoder{
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

func (r *Decoder) NextToken() (*models.Token, error) {
	v, err := func() (any, error) {
		b, err := _peek(r)
		if err != nil {
			return nil, err
		}

		var v any

		switch b {
		case markerGlobalSection:
			v, err = r.readGlobals()
			err = newSectionError(sectionGlobal, err)
		case markerRecordHeader:
			v, err = r.readRecord()
			err = newSectionError(sectionRecord, err)
		default:
			v, err = nil, fmt.Errorf("read invalid line start character %c", b)
		}

		return v, err
	}()

	if err != nil {
		return nil, newDecoderError(r.count, err)
	}

	switch v := v.(type) {
	case *models.SIndex:
		return models.NewSIndexToken(v), nil
	case *models.UDF:
		return models.NewUDFToken(v), nil
	case *models.Record:
		return models.NewRecordToken(*v), nil
	default:
		return nil, fmt.Errorf("unsupported token type %T", v)
	}
}

type header struct {
	Version string
}

func (r *Decoder) readHeader() (*header, error) {
	var res header

	if err := _expectToken(r, tokenASBVersion); err != nil {
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
func (r *Decoder) readMetadata() (*metaData, error) {
	var res metaData

	for {
		startC, err := _peek(r)
		if err != nil {
			return nil, err
		}

		// the metadata section is optional
		if startC != markerMetadataSection {
			break
		}

		if err := _expectChar(r, markerMetadataSection); err != nil {
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
		case tokenNamespace:
			if err := _expectChar(r, ' '); err != nil {
				return nil, err
			}

			val, err := r.readNamespace()
			if err != nil {
				return nil, newLineError(lineTypeNamespace, err)
			}

			res.Namespace = val

		case tokenFirstFile:
			val, err := r.readFirst()
			if err != nil {
				return nil, newLineError(lineTypeFirst, err)
			}

			res.First = val

		default:
			return nil, fmt.Errorf("unknown meta data line type %s", metaToken)
		}
	}

	return &res, nil
}

func (r *Decoder) readNamespace() (string, error) {
	data, err := _readUntil(r, '\n', true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return "", err
	}

	return string(data), nil
}

func (r *Decoder) readFirst() (bool, error) {
	if err := _expectChar(r, '\n'); err != nil {
		return false, err
	}

	return true, nil
}

func (r *Decoder) readGlobals() (any, error) {
	var res any

	if err := _expectChar(r, markerGlobalSection); err != nil {
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
	case globalTypeSIndex:
		res, err = r.readSIndex()
		if err != nil {
			return nil, newLineError(lineTypeSindex, err)
		}
	case globalTypeUDF:
		res, err = r.readUDF()
		if err != nil {
			return nil, newLineError(lineTypeUDF, err)
		}
	default:
		return nil, fmt.Errorf("invalid global line type %c", b)
	}

	return res, nil
}

// readSindex is used to read secondary index lines in the global section of the asb file.
// readSindex expects that r has been advanced past the secondary index global line markter '* i'
func (r *Decoder) readSIndex() (*models.SIndex, error) {
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

	indexType, err := r.readIndexType()
	if err != nil {
		return nil, err
	}

	res.IndexType = indexType

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

	binType, err := r.readBinType()
	if err != nil {
		return nil, err
	}

	path.BinType = binType

	// check for optional context
	b, err := _peek(r)
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

func (r *Decoder) readIndexType() (models.SIndexType, error) {
	b, err := r.ReadByte()
	if err != nil {
		return models.InvalidSIndex, err
	}

	switch b {
	case sindexTypeBin:
		return models.BinSIndex, nil
	case sindexTypeList:
		return models.ListElementSIndex, nil
	case sindexTypeMapKey:
		return models.MapKeySIndex, nil
	case sindexTypeMapVal:
		return models.MapValueSIndex, nil
	}

	return models.InvalidSIndex, fmt.Errorf("invalid secondary index type %c", b)
}

func (r *Decoder) readBinType() (models.SIPathBinType, error) {
	b, err := r.ReadByte()
	if err != nil {
		return models.InvalidSIDataType, err
	}

	switch b {
	case sindexBinTypeString:
		return models.StringSIDataType, nil
	case sindexBinTypeNumeric:
		return models.NumericSIDataType, nil
	case sindexBinTypeGEO2D:
		return models.GEO2DSphereSIDataType, nil
	case sindexBinTypeBlob:
		return models.BlobSIDataType, nil
	}

	return models.InvalidSIDataType, fmt.Errorf("invalid sindex path type %c", b)
}

// readUDF is used to read UDF lines in the global section of the asb file.
// readUDF expects that r has been advanced past the UDF global line marker '* u '
func (r *Decoder) readUDF() (*models.UDF, error) {
	var res models.UDF

	if err := _expectChar(r, ' '); err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch models.UDFType(b) {
	case models.UDFTypeLUA:
		res.UDFType = models.UDFTypeLUA
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

type recordData struct {
	userKey    any
	namespace  string
	set        string
	digest     []byte
	generation uint32
	binCount   uint16
	voidTime   int64
}

var expectedRecordHeaderTypes = []byte{
	recordHeaderTypeKey,
	recordHeaderTypeNamespace,
	recordHeaderTypeDigest,
	recordHeaderTypeSet,
	recordHeaderTypeGen,
	recordHeaderTypeExpiration,
	recordHeaderTypeBinCount,
}

func (r *Decoder) readRecord() (*models.Record, error) {
	var recData recordData

	for i := 0; i < len(expectedRecordHeaderTypes); i++ {
		if err := _expectChar(r, markerRecordHeader); err != nil {
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
		switch {
		case i == 0 && b == expectedRecordHeaderTypes[1]:
			i++
		case i == 3 && b == expectedRecordHeaderTypes[4]:
			i++
		case b != expectedRecordHeaderTypes[i]:
			return nil, fmt.Errorf("invalid record header line type %c expected %c", b, expectedRecordHeaderTypes[i])
		}

		if err := _expectChar(r, ' '); err != nil {
			return nil, err
		}

		if err := r.readRecordData(i, &recData); err != nil {
			return nil, err
		}
	}

	rec, err := r.prepareRecord(&recData)
	if err != nil {
		return nil, err
	}

	return &models.Record{
		Record:   rec,
		VoidTime: recData.voidTime,
	}, nil
}

func (r *Decoder) readRecordData(i int, recData *recordData) error {
	var err error

	switch i {
	case 0:
		recData.userKey, err = r.readUserKey()
	case 1:
		recData.namespace, err = r.readNamespace()
	case 2:
		recData.digest, err = r.readDigest()
	case 3:
		recData.set, err = r.readSet()
	case 4:
		recData.generation, err = r.readGeneration()
	case 5:
		recData.voidTime, err = r.readExpiration()
	case 6:
		recData.binCount, err = r.readBinCount()
	default:
		// should never happen because this is set to the length of expectedRecordHeaderTypes
		return fmt.Errorf("read too many record header lines, count: %d", i)
	}

	if err != nil {
		return newLineError(lineTypeKey, err)
	}

	return nil
}

func (r *Decoder) prepareRecord(recData *recordData) (*a.Record, error) {
	bins, err := r.readBins(recData.binCount)
	if err != nil {
		return nil, newLineError(lineTypeRecordBins, err)
	}

	key, err := a.NewKeyWithDigest(
		recData.namespace,
		recData.set,
		recData.userKey,
		recData.digest,
	)
	if err != nil {
		return nil, err
	}

	return &a.Record{
		Key:        key,
		Bins:       bins,
		Generation: recData.generation,
	}, nil
}

func (r *Decoder) readBins(count uint16) (a.BinMap, error) {
	bins := make(a.BinMap, count)

	for i := uint16(0); i < count; i++ {
		if err := _expectChar(r, markerRecordBins); err != nil {
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
	binTypeBytes:       {},
	binTypeBytesJava:   {},
	binTypeBytesCSharp: {},
	binTypeBytesPython: {},
	binTypeBytesRuby:   {},
	binTypeBytesPHP:    {},
	binTypeBytesErlang: {},
	binTypeBytesHLL:    {},
	binTypeBytesMap:    {},
	binTypeBytesList:   {},
}

// these are types that are stored as msgPack encoded bytes
var isMsgPackBytes = map[byte]struct{}{
	binTypeBytesHLL:  {},
	binTypeBytesMap:  {},
	binTypeBytesList: {},
}

var binTypes = map[byte]struct{}{
	// basic types
	binTypeNil:    {},
	binTypeBool:   {},
	binTypeInt:    {},
	binTypeFloat:  {},
	binTypeString: {},
	// bytes types
	binTypeBytes:       {},
	binTypeBytesJava:   {},
	binTypeBytesCSharp: {},
	binTypeBytesPython: {},
	binTypeBytesRuby:   {},
	binTypeBytesPHP:    {},
	binTypeBytesErlang: {},
	// bytes but parsed as another type
	binTypeBytesHLL:  {},
	binTypeBytesMap:  {},
	binTypeBytesList: {},
	// end bytes types
	binTypeLDT:          {},
	binTypeStringBase64: {},
	binTypeGeoJSON:      {},
}

func (r *Decoder) readBin(bins a.BinMap) error {
	binType, err := r.ReadByte()
	if err != nil {
		return err
	}

	if _, ok := binTypes[binType]; !ok {
		return fmt.Errorf("invalid bin type %c", binType)
	}

	base64Encoded, err := r.checkEncoded()
	if err != nil {
		return err
	}

	nameBytes, err := _readUntilAny(r, []byte{' ', '\n'}, true)
	if err != nil {
		return err
	}

	name := string(nameBytes)

	// binTypeNil is a special case where the line ends after the bin name
	if binType == binTypeNil {
		if err := _expectChar(r, '\n'); err != nil {
			return err
		}

		bins[name] = nil

		return nil
	}

	if err := _expectChar(r, ' '); err != nil {
		return err
	}

	binVal, binErr := fetchBinValue(r, binType, base64Encoded)

	if binErr != nil {
		return binErr
	}

	if err := _expectChar(r, '\n'); err != nil {
		return err
	}

	bins[name] = binVal

	return nil
}

func (r *Decoder) checkEncoded() (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}

	if b == ' ' {
		return true, nil
	}

	if b == '!' {
		if err := _expectChar(r, ' '); err != nil {
			return false, err
		}

		return false, nil
	}

	return false, fmt.Errorf("invalid character in bytes bin %c, expected '!' or ' '", b)
}

func fetchBinValue(r *Decoder, binType byte, base64Encoded bool) (any, error) {
	switch binType {
	case binTypeBool:
		return _readBool(r)
	case binTypeInt:
		return _readInteger(r, '\n')
	case binTypeFloat:
		return _readFloat(r, '\n')
	case binTypeString:
		return _readStringSized(r, ' ')
	case binTypeLDT:
		return nil, errors.New("this backup contains LDTs, please restore it using an older restore tool that supports LDTs")
	case binTypeStringBase64:
		val, err := _readBase64BytesSized(r, ' ')
		if err != nil {
			return nil, err
		}

		return string(val), nil
	case binTypeGeoJSON:
		return _readGeoJSON(r, ' ')
	}

	if _, ok := bytesBinTypes[binType]; !ok {
		return nil, fmt.Errorf("unexpected binType %d", binType)
	}

	var (
		val []byte
		err error
	)

	if base64Encoded {
		val, err = _readBase64BytesSized(r, ' ')
	} else {
		val, err = _readBytesSized(r, ' ')
	}

	if err != nil {
		return nil, err
	}

	if _, ok := isMsgPackBytes[binType]; !ok {
		return val, nil
	}

	switch binType {
	case binTypeBytesHLL:
		return a.NewHLLValue(val), nil
	case binTypeBytesMap:
		return a.NewRawBlobValue(particleType.MAP, val), nil
	case binTypeBytesList:
		return a.NewRawBlobValue(particleType.LIST, val), nil
	}

	return nil, fmt.Errorf("invalid bytes to type binType %d", binType)
}

var asbKeyTypes = map[byte]struct{}{
	keyTypeInt:          {}, // int64
	keyTypeFloat:        {}, // float64
	keyTypeString:       {}, // string
	keyTypeStringBase64: {}, // base64 encoded string
	keyTypeBytes:        {}, // bytes
}

// readUserKey reads a record key line from the asb file
// it expects that r has been advanced past the record key line marker '+ k'
func (r *Decoder) readUserKey() (any, error) {
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
	case keyTypeInt:
		keyVal, err := _readInteger(r, '\n')
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeFloat:
		keyVal, err := _readFloat(r, '\n')
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeString:
		keyVal, err := _readStringSized(r, ' ')
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeStringBase64:
		keyVal, err := _readBase64BytesSized(r, ' ')
		if err != nil {
			return nil, err
		}

		res = string(keyVal)

	case keyTypeBytes:
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

func (r *Decoder) readBinCount() (uint16, error) {
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
func (r *Decoder) readExpiration() (int64, error) {
	exp, err := _readInteger(r, '\n')
	if err != nil {
		return 0, err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return 0, err
	}

	if exp < 0 {
		return 0, fmt.Errorf("invalid expiration time %d", exp)
	}

	return exp, nil
}

func (r *Decoder) readGeneration() (uint32, error) {
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

func (r *Decoder) readSet() (string, error) {
	set, err := _readUntil(r, '\n', true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r, '\n'); err != nil {
		return "", err
	}

	return string(set), err
}

func (r *Decoder) readDigest() ([]byte, error) {
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
	data, err := _readNBytes(src, n)
	if err != nil {
		return nil, err
	}

	return _decodeBase64(data)
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
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(string(data), 64)
}

func _readInteger(src io.ByteScanner, delim byte) (int64, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(data), 10, 64)
}

func _readGeoJSON(src io.ByteScanner, sizeDelim byte) (a.GeoJSONValue, error) {
	val, err := _readStringSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	return a.NewGeoJSONValue(val), nil
}

func _readHLL(src io.ByteScanner, sizeDelim byte) (a.HLLValue, error) {
	data, err := _readBytesSized(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	return a.NewHLLValue(data), nil
}

// _readSize reads a size or length token from the asb format
// the value should fit in a uint32
func _readSize(src io.ByteScanner, delim byte) (uint32, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(string(data), 10, 32)

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
	data := make([]byte, n)

	for i := 0; i < n; i++ {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		data[i] = b
	}

	return data, nil
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
		}

		return fmt.Errorf("invalid character, read %c, expected one of %s", b, string(chars))
	}

	return nil
}

func _expectToken(src io.ByteReader, token string) error {
	data, err := _readNBytes(src, len(token))
	if err != nil {
		return err
	}

	if string(data) != token {
		return fmt.Errorf("invalid token, read %s, expected %s", string(data), token)
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
