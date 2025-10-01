// Copyright 2024 Aerospike, Inc.
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
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
)

var errInvalidToken = errors.New("invalid token")

// The following sync.Pool instances provide optimized memory reuse
// for byte slices of varying capacities.
var (
	// bigBufPool is a pool of big byte slices used for decoding.
	bigBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 512)
		},
	}
	// smallBufPool is a pool of small byte slices used for decoding.
	smallBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 64)
		},
	}
)

// returnBigBuffer return buffer to pool.
func returnBigBuffer(buf []byte) {
	// Reset length but keep capacity
	//nolint:staticcheck // We try to decrease allocation, not to make them zero.
	bigBufPool.Put(buf[:0])
}

// returnSmallBuffer return buffer to pool.
func returnSmallBuffer(buf []byte) {
	// Reset length but keep capacity
	//nolint:staticcheck // We try to decrease allocation, not to make them zero.
	smallBufPool.Put(buf[:0])
}

func newDecoderError(tracker *positionTracker, err error) error {
	if errors.Is(err, io.EOF) {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf(
		"error while reading asb data: %s line %d col %d (total byte %d): %w",
		tracker.fileName,
		tracker.line,
		tracker.column,
		tracker.offset,
		err,
	)
}

func newSectionError(section string, err error) error {
	if errors.Is(err, io.EOF) {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("error while reading section: %s: %w", section, err)
}

func newLineError(lineType string, err error) error {
	if errors.Is(err, io.EOF) {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("error while reading line type: %s: %w", lineType, err)
}

// positionTracker is used for tracking error information when validating backup files.
type positionTracker struct {
	fileName string
	offset   uint64
	line     int64
	column   int64
	prevByte byte
	prevCol  int64
}

// countingReader represents a wrapper for fast reading.
// It keeps track of the number of bytes read.
type countingReader struct {
	*bufio.Reader
	tracker *positionTracker
}

func newCountingReader(src io.Reader, fileName string) *countingReader {
	return &countingReader{
		Reader: bufio.NewReader(src),
		tracker: &positionTracker{
			fileName: fileName,
			// For printing lines starting from 1.
			line: 1,
		},
	}
}

// ReadByte reads a single byte from the underlying reader.
func (c *countingReader) ReadByte() (byte, error) {
	b, err := c.Reader.ReadByte()
	if err != nil {
		return 0, err
	}

	c.tracker.offset++

	// If it is a new line byte.
	if b == asbNewLine {
		// Increase line counter.
		c.tracker.line++
		// Save the previous column counter, so we can return in case of Unread.
		c.tracker.prevCol = c.tracker.column
		// Reset column counter.
		c.tracker.column = 0
	} else {
		// If no new line, just move the column counter.
		c.tracker.column++
	}
	// Save the previous value, so we can track changes on Unread.
	c.tracker.prevByte = b

	return b, nil
}

// UnreadByte unreads a single byte from the underlying reader.
func (c *countingReader) UnreadByte() error {
	err := c.Reader.UnreadByte()
	if err != nil {
		return err
	}

	// Check if the previous byte was asbNewLine.
	if c.tracker.prevByte == asbNewLine {
		// We return one step back.
		c.tracker.line--
		c.tracker.column = c.tracker.prevCol
	}

	c.tracker.offset--

	return nil
}

type metaData struct {
	Namespace string
	First     bool
}

// Decoder contains logic for decoding backup data from the .asb format.
type Decoder[T models.TokenConstraint] struct {
	header   *header
	metaData *metaData
	reader   *countingReader
}

// NewDecoder creates a new Decoder.
func NewDecoder[T models.TokenConstraint](src io.Reader, fileName string) (*Decoder[T], error) {
	var err error

	asb := Decoder[T]{
		reader: newCountingReader(src, fileName),
	}

	asb.header, err = asb.readHeader()

	switch {
	case err == nil: // ok
	case errors.Is(err, errInvalidToken):
		return nil, fmt.Errorf("error while reading %s header: %w. "+
			"This may happen if the file was compressed/encrypted and the restore config does not"+
			" contain the proper compression/encryption policy, or the file is corrupted", fileName, err)
	default:
		return nil, fmt.Errorf("error while reading %s header: %w", fileName, err)
	}

	fileVersion, err := parseVersion(asb.header.Version)
	if err != nil {
		return nil, fmt.Errorf("error while parsing %s header version: %w", fileName, err)
	}

	if !versionCurrent.greaterOrEqual(fileVersion) {
		return nil, fmt.Errorf("unsupported backup file version: %s", asb.header.Version)
	}

	asb.metaData, err = asb.readMetadata()
	if err != nil {
		return nil, fmt.Errorf("error while reading metadata: %w", err)
	}

	return &asb, nil
}

func (r *Decoder[T]) NextToken() (T, error) {
	countBefore := r.reader.tracker.offset

	v, err := func() (any, error) {
		b, err := _peek(r.reader)
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
		return nil, newDecoderError(r.reader.tracker, err)
	}

	size := r.reader.tracker.offset - countBefore

	var t *models.Token
	switch v := v.(type) {
	case *models.SIndex:
		t = models.NewSIndexToken(v, size)
	case *models.UDF:
		t = models.NewUDFToken(v, size)
	case *models.Record:
		t = models.NewRecordToken(v, size, nil)
	default:
		return nil, fmt.Errorf("unsupported token type %T", v)
	}

	return any(t).(T), nil
}

type header struct {
	Version string
}

func (r *Decoder[T]) readHeader() (*header, error) {
	var res header

	if err := _expectToken(r.reader, tokenASBVersion); err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	// version number format is "x.y"
	ver, err := _readNBytes(r.reader, 3)
	if err != nil {
		return nil, err
	}

	res.Version = string(ver)

	returnBigBuffer(ver)

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return &res, nil
}

// readMetadata consumes all metadata lines
func (r *Decoder[T]) readMetadata() (*metaData, error) {
	var res metaData

	for {
		startC, err := _peek(r.reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		// the metadata section is optional
		if startC != markerMetadataSection {
			break
		}

		if err := _expectChar(r.reader, markerMetadataSection); err != nil {
			return nil, err
		}

		if err := _expectChar(r.reader, ' '); err != nil {
			return nil, err
		}

		metaToken, err := _readUntilAny(r.reader, []byte{' ', asbNewLine}, false)
		if err != nil {
			return nil, err
		}

		mToken := string(metaToken)
		returnSmallBuffer(metaToken)

		switch mToken {
		case tokenNamespace:
			if err := _expectChar(r.reader, ' '); err != nil {
				return nil, err
			}

			res.Namespace, err = r.readNamespace()
			if err != nil {
				return nil, newLineError(lineTypeNamespace, err)
			}

		case tokenFirstFile:
			res.First, err = r.readFirst()
			if err != nil {
				return nil, newLineError(lineTypeFirst, err)
			}

		default:
			return nil, fmt.Errorf("unknown meta data line type %s", mToken)
		}
	}

	return &res, nil
}

func (r *Decoder[T]) readNamespace() (string, error) {
	data, err := _readUntil(r.reader, asbNewLine, true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return "", err
	}

	return data, nil
}

func (r *Decoder[T]) readFirst() (bool, error) {
	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return false, err
	}

	return true, nil
}

func (r *Decoder[T]) readGlobals() (any, error) {
	var res any

	if err := _expectChar(r.reader, markerGlobalSection); err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	b, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case globalTypeSIndex:
		res, err = r.readSIndex(false)
		if err != nil {
			return nil, newLineError(lineTypeSindex, err)
		}
	case globalTypeSIndexExpression:
		res, err = r.readSIndex(true)
		if err != nil {
			return nil, newLineError(lineTypeSindex, err)
		}
	case globalTypeUDF:
		res, err = r.readUDF()
		if err != nil {
			return nil, newLineError(lineTypeUDF, err)
		}
	default:
		// Skip unknown global type - read until newline.
		if err = r.skipToNextLine(); err != nil {
			return nil, fmt.Errorf("failed to skip unknown global line type %c: %w", b, err)
		}

		return nil, fmt.Errorf("invalid global line type %c", b)
	}

	return res, nil
}

// readSIndex is used to read secondary index lines in the global section of the asb file.
// readSIndex expects that r has been advanced past the secondary index global line marker '* i' or '* e'
// If isExpression = true, we assume it is sindex with expression.
//
//nolint:gocyclo // Long decoding func
func (r *Decoder[T]) readSIndex(isExpression bool) (*models.SIndex, error) {
	var (
		res models.SIndex
		err error
	)

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Namespace, err = _readUntil(r.reader, ' ', true)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Set, err = _readUntil(r.reader, ' ', true)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Name, err = _readUntil(r.reader, ' ', true)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.IndexType, err = r.readSIndexType()
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	// NOTE: the number of paths is always 1 for now
	// this means we read the value but don't use it
	npaths, err := _readSize(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if npaths == 0 {
		return nil, errors.New("missing path(s) in sindex block")
	}

	var path models.SIndexPath

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	path.BinName, err = _readUntil(r.reader, ' ', true)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	path.BinType, err = r.readSIndexBinType()
	if err != nil {
		return nil, err
	}

	// check for optional context
	b, err := _peek(r.reader)
	if err != nil {
		return nil, err
	}

	if b == ' ' {
		if err := _expectChar(r.reader, ' '); err != nil {
			return nil, err
		}
		// Expression filter has a base64 encoded expression and no CDT context.
		// If it is not expression, we assume it is CDT contex.
		if isExpression {
			res.Expression, err = _readUntil(r.reader, asbNewLine, false)
			if err != nil {
				return nil, err
			}
		} else {
			// NOTE: the context should always be base64 encoded,
			// so escaping is not needed
			path.B64Context, err = _readUntil(r.reader, asbNewLine, false)
			if err != nil {
				return nil, err
			}
		}
	}

	res.Path = path

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return &res, nil
}

func (r *Decoder[T]) readSIndexType() (models.SIndexType, error) {
	b, err := r.reader.ReadByte()
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

func (r *Decoder[T]) readSIndexBinType() (models.SIPathBinType, error) {
	b, err := r.reader.ReadByte()
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
func (r *Decoder[T]) readUDF() (*models.UDF, error) {
	var (
		res models.UDF
	)

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	b, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch models.UDFType(b) {
	case models.UDFTypeLUA:
		res.UDFType = models.UDFTypeLUA
	default:
		return nil, fmt.Errorf("invalid UDF type %c in global section UDF line", b)
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Name, err = _readUntil(r.reader, ' ', true)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	length, err := _readSize(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	content, err := _readNBytes(r.reader, int64(length))
	if err != nil {
		return nil, err
	}

	res.Content = make([]byte, len(content))
	copy(res.Content, content)
	returnBigBuffer(content)

	if err := _expectChar(r.reader, asbNewLine); err != nil {
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

func (r *Decoder[T]) readRecord() (*models.Record, error) {
	var recData recordData

	for i := 0; i < len(expectedRecordHeaderTypes); i++ {
		if err := _expectChar(r.reader, markerRecordHeader); err != nil {
			return nil, err
		}

		if err := _expectChar(r.reader, ' '); err != nil {
			return nil, err
		}

		b, err := r.reader.ReadByte()
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

		if err := _expectChar(r.reader, ' '); err != nil {
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

func (r *Decoder[T]) readRecordData(i int, recData *recordData) error {
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
		return fmt.Errorf("read too many record header lines, offset: %d", i)
	}

	if err != nil {
		return newLineError(lineTypeKey, err)
	}

	return nil
}

func (r *Decoder[T]) prepareRecord(recData *recordData) (*a.Record, error) {
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

func (r *Decoder[T]) readBins(count uint16) (a.BinMap, error) {
	bins := make(a.BinMap, count)

	for i := uint16(0); i < count; i++ {
		if err := _expectChar(r.reader, markerRecordBins); err != nil {
			return nil, err
		}

		if err := _expectChar(r.reader, ' '); err != nil {
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

func (r *Decoder[T]) readBin(bins a.BinMap) error {
	binType, err := r.reader.ReadByte()
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

	nameBytes, err := _readUntilAny(r.reader, []byte{' ', asbNewLine}, true)
	if err != nil {
		return err
	}

	name := string(nameBytes)
	returnSmallBuffer(nameBytes)

	// binTypeNil is a special case where the line ends after the bin name
	if binType == binTypeNil {
		if err := _expectChar(r.reader, asbNewLine); err != nil {
			return err
		}

		bins[name] = nil

		return nil
	}

	if err := _expectChar(r.reader, ' '); err != nil {
		return err
	}

	binVal, binErr := fetchBinValue(r, binType, base64Encoded)

	if binErr != nil {
		return binErr
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return err
	}

	bins[name] = binVal

	return nil
}

func (r *Decoder[T]) checkEncoded() (bool, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return false, err
	}

	if b == ' ' {
		return true, nil
	}

	if b == '!' {
		if err := _expectChar(r.reader, ' '); err != nil {
			return false, err
		}

		return false, nil
	}

	return false, fmt.Errorf("invalid character %c, expected '!' or ' '", b)
}

func fetchBinValue[T models.TokenConstraint](r *Decoder[T], binType byte, base64Encoded bool) (any, error) {
	switch binType {
	case binTypeBool:
		return _readBool(r.reader)
	case binTypeInt:
		return _readInteger(r.reader, asbNewLine)
	case binTypeFloat:
		return _readFloat(r.reader, asbNewLine)
	case binTypeString:
		return _readStringSized(r.reader, ' ')
	case binTypeLDT:
		return nil, errors.New("this backup contains LDTs, please restore it using an older restore tool that supports LDTs")
	case binTypeStringBase64:
		val, err := _readBase64BytesSized(r.reader, ' ')
		if err != nil {
			return nil, err
		}

		result := string(val)
		returnBase64Buffer(val)

		return result, nil
	case binTypeGeoJSON:
		return _readGeoJSON(r.reader, ' ')
	}

	if _, ok := bytesBinTypes[binType]; !ok {
		return nil, fmt.Errorf("unexpected binType %d", binType)
	}

	var (
		val []byte
		err error
	)

	if base64Encoded {
		val, err = _readBase64BytesSized(r.reader, ' ')
		defer returnBase64Buffer(val)
	} else {
		val, err = _readBytesSized(r.reader, ' ')
		defer returnBigBuffer(val)
	}

	if err != nil {
		return nil, err
	}

	if _, ok := isMsgPackBytes[binType]; !ok {
		// We should copy a result, as buffer will be returned.
		mspVal := make([]byte, len(val))
		copy(mspVal, val)

		return mspVal, nil
	}

	switch binType {
	case binTypeBytesHLL:
		// Only HLL val is not copied inside the lib, so we should copy it ourselves.
		hllVal := make([]byte, len(val))
		copy(hllVal, val)

		return a.NewHLLValue(hllVal), nil
	case binTypeBytesMap:
		return a.NewRawBlobValue(particleType.MAP, val), nil
	case binTypeBytesList:
		return a.NewRawBlobValue(particleType.LIST, val), nil
	default:
		return nil, fmt.Errorf("invalid bytes to type binType %d", binType)
	}
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
func (r *Decoder[T]) readUserKey() (any, error) {
	var res any

	keyTypeChar, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if _, ok := asbKeyTypes[keyTypeChar]; !ok {
		return nil, fmt.Errorf("invalid key type %c", keyTypeChar)
	}

	b, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	// handle the special case where a byte key is not base64 encoded
	var base64Encoded bool

	switch b {
	case '!':
	case ' ':
		base64Encoded = true
	default:
		return nil, fmt.Errorf("invalid character %c, expected '!' or ' '", keyTypeChar)
	}

	if !base64Encoded {
		if err := _expectChar(r.reader, ' '); err != nil {
			return nil, err
		}
	}

	switch keyTypeChar {
	case keyTypeInt:
		keyVal, err := _readInteger(r.reader, asbNewLine)
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeFloat:
		keyVal, err := _readFloat(r.reader, asbNewLine)
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeString:
		keyVal, err := _readStringSized(r.reader, ' ')
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeStringBase64:
		keyVal, err := _readBase64BytesSized(r.reader, ' ')
		if err != nil {
			return nil, err
		}

		res = string(keyVal)

		returnBase64Buffer(keyVal)

	case keyTypeBytes:
		var keyVal, cVal []byte
		if base64Encoded {
			keyVal, err = _readBase64BytesSized(r.reader, ' ')
			cVal = make([]byte, len(keyVal))
			copy(cVal, keyVal)
			returnBase64Buffer(keyVal)
		} else {
			keyVal, err = _readBytesSized(r.reader, ' ')
			cVal = make([]byte, len(keyVal))
			copy(cVal, keyVal)
			returnBigBuffer(keyVal)
		}

		if err != nil {
			return nil, err
		}

		res = cVal

	default:
		// should never happen because of the previous check for membership in asbKeyTypes
		return nil, fmt.Errorf("invalid key type %c", keyTypeChar)
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return res, nil
}

func (r *Decoder[T]) readBinCount() (uint16, error) {
	binCount, err := _readInteger(r.reader, asbNewLine)
	if err != nil {
		return 0, err
	}

	if binCount > maxBinCount || binCount < 0 {
		return 0, fmt.Errorf("invalid bin offset %d", binCount)
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return 0, err
	}

	return uint16(binCount), nil
}

// readExpiration reads an expiration line from the asb file
// it expects that r has been advanced past the expiration line marker '+ t '
// NOTE: we don't check the expiration against any bounds because negative (large) expirations are valid
func (r *Decoder[T]) readExpiration() (int64, error) {
	exp, err := _readInteger(r.reader, asbNewLine)
	if err != nil {
		return 0, err
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return 0, err
	}

	if exp < 0 {
		return 0, fmt.Errorf("invalid expiration time %d", exp)
	}

	return exp, nil
}

func (r *Decoder[T]) readGeneration() (uint32, error) {
	gen, err := _readInteger(r.reader, asbNewLine)
	if err != nil {
		return 0, err
	}

	if gen < 0 || gen > maxGeneration {
		return 0, fmt.Errorf("invalid generation offset %d", gen)
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return 0, err
	}

	return uint32(gen), nil
}

func (r *Decoder[T]) readSet() (string, error) {
	set, err := _readUntil(r.reader, asbNewLine, true)
	if err != nil {
		return "", err
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return "", err
	}

	return set, err
}

func (r *Decoder[T]) readDigest() ([]byte, error) {
	digest, err := _readBase64BytesDelimited(r.reader, asbNewLine)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return digest, nil
}

func (r *Decoder[T]) skipToNextLine() error {
	// Read until newline, no escaping needed for skip.
	buf, err := _readUntilAny(r.reader, []byte{asbNewLine}, false)
	if err != nil {
		return err
	}

	// Return buffer to pool immediately since we don't need the data.
	returnSmallBuffer(buf)

	// Consume the newline.
	_, err = r.reader.ReadByte()

	return err
}

// ***** Helper Functions

func _readBase64BytesDelimited(src *countingReader, delim byte) ([]byte, error) {
	encoded, err := _readUntilAny(src, []byte{delim}, false)
	if err != nil {
		return nil, err
	}

	result, err := _decodeBase64(encoded)
	if err != nil {
		return nil, err
	}

	returnSmallBuffer(encoded)

	decoded := make([]byte, len(result))
	copy(decoded, result)

	returnBase64Buffer(result)

	return decoded, nil
}

func _readBase64BytesSized(src *countingReader, sizeDelim byte) ([]byte, error) {
	size, err := _readSize(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	return _readBlockDecodeBase64(src, int64(size))
}

func _readBlockDecodeBase64(src *countingReader, n int64) ([]byte, error) {
	data, err := _readNBytes(src, n)
	if err != nil {
		return nil, err
	}

	result, err := _decodeBase64(data)
	if err != nil {
		return nil, err
	}

	returnSmallBuffer(data)

	return result, nil
}

func _decodeBase64(src []byte) ([]byte, error) {
	decodedLen := base64.StdEncoding.DecodedLen(len(src))

	// Get a buffer from the pool
	buf := base64BufferPool.Get().([]byte)

	// Ensure the buffer is large enough
	if cap(buf) < decodedLen {
		// If the buffer is too small, create a new one with sufficient capacity
		buf = make([]byte, decodedLen)
	} else {
		// Otherwise, resize the existing buffer
		buf = buf[:decodedLen]
	}

	bw, err := base64.StdEncoding.Decode(buf, src)
	if err != nil {
		return nil, err
	}

	return buf[:bw], nil
}

func _readStringSized(src *countingReader, sizeDelim byte) (string, error) {
	val, err := _readBytesSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	result := string(val)

	returnBigBuffer(val)

	return result, nil
}

func _readBytesSized(src *countingReader, sizeDelim byte) ([]byte, error) {
	length, err := _readSize(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	if err := _expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	return _readNBytes(src, int64(length))
}

func _readBool(src *countingReader) (bool, error) {
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

func _readFloat(src *countingReader, delim byte) (float64, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(data, 64)
}

func _readInteger(src *countingReader, delim byte) (int64, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(data, 10, 64)
}

func _readGeoJSON(src *countingReader, sizeDelim byte) (a.GeoJSONValue, error) {
	val, err := _readStringSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	return a.NewGeoJSONValue(val), nil
}

func _readHLL(src *countingReader, sizeDelim byte) (a.HLLValue, error) {
	data, err := _readBytesSized(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	result := a.NewHLLValue(data)

	returnBigBuffer(data)

	return result, nil
}

// _readSize reads a size or length token from the asb format
// the value should fit in a uint32
func _readSize(src *countingReader, delim byte) (uint32, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(data, 10, 32)

	return uint32(num), err
}

func _readUntil(src *countingReader, delim byte, escaped bool) (string, error) {
	result, err := _readUntilAny(src, []byte{delim}, escaped)
	if err != nil {
		return "", err
	}

	resultStr := string(result)

	returnSmallBuffer(result)

	return resultStr, nil
}

func _readUntilAny(src *countingReader, delims []byte, escaped bool) ([]byte, error) {
	bufInterface := smallBufPool.Get()
	buf := bufInterface.([]byte)

	var esc bool

	for i := 0; i < maxTokenSize; i++ {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		if escaped && b == asbEscape && !esc {
			esc = true
			continue
		}

		if !esc {
			for _, delim := range delims {
				if b == delim {
					return buf, src.UnreadByte()
				}
			}
		}

		esc = false

		buf = append(buf, b)
	}

	return nil, errors.New("token larger than max size")
}

func _readNBytes(src *countingReader, n int64) ([]byte, error) {
	buf := bigBufPool.Get().([]byte)

	// Ensure the buffer is large enough
	if int64(cap(buf)) < n {
		// If the buffer is too small, create a new one with sufficient capacity
		buf = make([]byte, n)
	} else {
		// Otherwise, resize the existing buffer
		buf = buf[:n]
	}

	_, err := io.ReadFull(src, buf)
	if err != nil {
		return nil, err
	}

	// Increase global offset.
	src.tracker.offset += uint64(n)

	// Update position tracker by counting newlines in the read data, only if we found at least one newline.
	if bytes.IndexByte(buf, asbNewLine) != -1 {
		// Us bytes.Count for fast counting of newlines.
		newlineCount := bytes.Count(buf, []byte{asbNewLine})
		// Increase counter.
		src.tracker.line += int64(newlineCount)

		if newlineCount > 0 {
			src.tracker.column = 0
		} else {
			src.tracker.column += n
		}
	} else {
		src.tracker.column += n
	}

	// Set previous byte as last byte of the read data.
	if n > 0 {
		src.tracker.prevByte = buf[n-1]
	}

	return buf, nil
}

func _expectChar(src *countingReader, c byte) error {
	return _expectAnyChar(src, []byte{c})
}

func _expectAnyChar(src *countingReader, chars []byte) error {
	b, err := src.ReadByte()
	if err != nil {
		return err
	}

	for _, c := range chars {
		if b == c {
			return nil
		}
	}

	if len(chars) == 1 {
		return fmt.Errorf("invalid character, read %c, expected %c", b, chars[0])
	}

	return fmt.Errorf("invalid character, read %c, expected one of %s", b, string(chars))
}

func _expectToken(src *countingReader, token string) error {
	data, err := _readNBytes(src, int64(len(token)))
	if err != nil {
		return err
	}

	result := string(data)
	returnBigBuffer(data)

	if result != token {
		return fmt.Errorf("%w, read %s, expected %s", errInvalidToken, string(data), token)
	}

	return nil
}

func _peek(src *countingReader) (byte, error) {
	b, err := src.ReadByte()
	if err != nil {
		return 0, err
	}

	err = src.UnreadByte()

	return b, err
}
