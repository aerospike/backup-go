// Copyright 2024-2026 Aerospike, Inc.
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
	"log/slog"
	"strconv"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/io/compression"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
)

var errInvalidToken = fmt.Errorf("%w: invalid token", errclass.ErrCorruptData)

func newDecoderError(tracker *positionTracker, err error) error {
	if errors.Is(err, io.EOF) {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf(
		"failed to read asb data: %s line %d col %d (total byte %d): %w",
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

	return fmt.Errorf("failed to read section %s: %w", section, err)
}

func newLineError(lineType string, err error) error {
	if errors.Is(err, io.EOF) {
		return err
	} else if err == nil {
		return nil
	}

	return fmt.Errorf("failed to read line type %s: %w", lineType, err)
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
		Reader: bufio.NewReaderSize(src, 1024*1024), // 1mb buffer
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

	c.tracker.note(b)

	return b, nil
}

// note records one consumed byte in the position tracker.
func (t *positionTracker) note(b byte) {
	t.offset++

	if b == asbNewLine {
		t.line++
		t.prevCol = t.column
		t.column = 0
	} else {
		t.column++
	}

	t.prevByte = b
}

// noteBytes records a sequence of consumed bytes.
func (t *positionTracker) noteBytes(p []byte) {
	for _, b := range p {
		t.note(b)
	}
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
type Decoder struct {
	header   *header
	metaData *metaData
	reader   *countingReader
	// If set to true, unknown global/record types will be ignored and the decoder will continue to read the next line.
	ignoreUnknownFields bool
	logger              *slog.Logger
}

// NewDecoder creates a new Decoder.
func NewDecoder(src io.Reader, fileName string, ignoreUnknownFields bool, logger *slog.Logger,
) (*Decoder, error) {
	var err error

	asb := Decoder{
		reader:              newCountingReader(src, fileName),
		ignoreUnknownFields: ignoreUnknownFields,
		logger:              logger,
	}

	asb.header, err = asb.readHeader()

	switch {
	case err == nil: // ok
	case errors.Is(err, errInvalidToken),
		compression.IsCorruptedError(err):
		return nil, fmt.Errorf("failed to read %s header: %w. "+
			"this may happen if the file was compressed/encrypted and the restore config does not"+
			" contain the proper compression/encryption policy, or the file is corrupted", fileName, err)
	default:
		return nil, fmt.Errorf("failed to read %s header: %w", fileName, err)
	}

	fileVersion, err := parseVersion(asb.header.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header version: %w", fileName, err)
	}

	if !versionCurrent.greaterOrEqual(fileVersion) {
		return nil, fmt.Errorf("%w: unsupported backup file version: %s", errclass.ErrUnsupported, asb.header.Version)
	}

	asb.metaData, err = asb.readMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	return &asb, nil
}

func (r *Decoder) NextToken() (*models.Token, error) {
	countBefore := r.reader.tracker.offset

	v, err := func() (any, error) {
		b, err := peek(r.reader)
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
			v, err = nil, fmt.Errorf("%w: read invalid line start character %c", errclass.ErrCorruptData, b)
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
		return nil, fmt.Errorf("%w: unsupported token type %T", errclass.ErrUnsupported, v)
	}

	return any(t).(*models.Token), nil
}

type header struct {
	Version string
}

func (r *Decoder) readHeader() (*header, error) {
	var res header

	if err := expectToken(r.reader, tokenASBVersion); err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	// version number format is "x.y"
	ver, err := readNBytes(r.reader, 3)
	if err != nil {
		return nil, err
	}

	res.Version = string(ver)

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return &res, nil
}

// readMetadata consumes all metadata lines
func (r *Decoder) readMetadata() (*metaData, error) {
	var res metaData

	for {
		startC, err := peek(r.reader)
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

		if err := expectChar(r.reader, markerMetadataSection); err != nil {
			return nil, err
		}

		if err := expectChar(r.reader, ' '); err != nil {
			return nil, err
		}

		metaToken, err := readUntilWhitespace(r.reader)
		if err != nil {
			return nil, err
		}

		mToken := string(metaToken)

		switch mToken {
		case tokenNamespace:
			if err := expectChar(r.reader, ' '); err != nil {
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
			return nil, fmt.Errorf("%w: unknown meta data line type %s", errclass.ErrCorruptData, mToken)
		}
	}

	return &res, nil
}

func (r *Decoder) readNamespace() (string, error) {
	data, err := readUntilEscaped(r.reader, asbNewLine)
	if err != nil {
		return "", err
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return "", err
	}

	return data, nil
}

func (r *Decoder) readFirst() (bool, error) {
	if err := expectChar(r.reader, asbNewLine); err != nil {
		return false, err
	}

	return true, nil
}

func (r *Decoder) readGlobals() (any, error) {
	var res any

	if err := expectChar(r.reader, markerGlobalSection); err != nil {
		return r.skipAndRetryGlobals(fmt.Errorf("failed to read global section: %w", err))
	}

	if err := expectChar(r.reader, ' '); err != nil {
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
		return r.skipAndRetryGlobals(fmt.Errorf("failed to read global line type %c: %w", b, err))
	}

	return res, nil
}

func (r *Decoder) skipAndRetryGlobals(err error) (any, error) {
	if !r.ignoreUnknownFields {
		return nil, err
	}

	if err := r.skipToNextLine(); err != nil {
		return nil, fmt.Errorf("failed to skip: %w", err)
	}

	r.logger.Warn("skipping unknown section",
		slog.Any("error", err))

	return r.readGlobals()
}

// readSIndex is used to read secondary index lines in the global section of the asb file.
// readSIndex expects that r has been advanced past the secondary index global line marker '* i' or '* e'
// If isExpression = true, we assume it is sindex with expression.
//
//nolint:gocyclo // Long decoding func
func (r *Decoder) readSIndex(isExpression bool) (*models.SIndex, error) {
	var (
		res models.SIndex
		err error
	)

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Namespace, err = readUntilEscaped(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Set, err = readUntilEscaped(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Name, err = readUntilEscaped(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.IndexType, err = r.readSIndexType()
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	// NOTE: the number of paths is always 1 for now
	// this means we read the value but don't use it
	npaths, err := readUnsignedInt(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if npaths == 0 {
		return nil, fmt.Errorf("%w: missing path(s) in sindex block", errclass.ErrCorruptData)
	}

	var path models.SIndexPath

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	path.BinName, err = readUntilEscaped(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	path.BinType, err = r.readSIndexBinType()
	if err != nil {
		return nil, err
	}

	// check for optional context
	b, err := peek(r.reader)
	if err != nil {
		return nil, err
	}

	if b == ' ' { //nolint:nestif // optional context: two clear branches (expression vs CDT context)
		if err := expectChar(r.reader, ' '); err != nil {
			return nil, err
		}
		// Expression filter has a base64 encoded expression and no CDT context.
		// If it is not expression, we assume it is CDT context.
		if isExpression {
			res.Expression, err = readUntil(r.reader, asbNewLine)
			if err != nil {
				return nil, err
			}
		} else {
			// NOTE: the context should always be base64 encoded,
			// so escaping is not needed
			path.B64Context, err = readUntil(r.reader, asbNewLine)
			if err != nil {
				return nil, err
			}
		}
	}

	res.Path = path

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return &res, nil
}

func (r *Decoder) readSIndexType() (models.SIndexType, error) {
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
	case sindexTypeSet:
		return models.SetSIndex, nil
	}

	return models.InvalidSIndex, fmt.Errorf("%w: invalid secondary index type %c", errclass.ErrCorruptData, b)
}

func (r *Decoder) readSIndexBinType() (models.SIPathBinType, error) {
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
	case sindexBinTypeEmpty:
		return models.EmptySIDataType, nil
	}

	return models.InvalidSIDataType, fmt.Errorf("%w: invalid sindex path type %c", errclass.ErrCorruptData, b)
}

// readUDF is used to read UDF lines in the global section of the asb file.
// readUDF expects that r has been advanced past the UDF global line marker '* u '
func (r *Decoder) readUDF() (*models.UDF, error) {
	var (
		res models.UDF
	)

	if err := expectChar(r.reader, ' '); err != nil {
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
		return nil, fmt.Errorf("%w: invalid UDF type %c in global section UDF line", errclass.ErrCorruptData, b)
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	res.Name, err = readUntilEscaped(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	length, err := readUnsignedInt(r.reader, ' ')
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return nil, err
	}

	content, err := readNBytes(r.reader, int64(length))
	if err != nil {
		return nil, err
	}

	// content is already a fresh allocation from readNBytes, use directly
	res.Content = content

	if err := expectChar(r.reader, asbNewLine); err != nil {
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
		if err := expectChar(r.reader, markerRecordHeader); err != nil {
			return nil, err
		}

		if err := expectChar(r.reader, ' '); err != nil {
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
			if r.ignoreUnknownFields {
				// Skip only this field.
				if err := r.skipToNextLine(); err != nil {
					return nil, fmt.Errorf("%w: failed to skip unknown record header type %c: %w", errclass.ErrCorruptData, b, err)
				}

				r.logger.Warn("ignoring error while reading record field type",
					slog.Any("error",
						fmt.Errorf("invalid record header line type %c expected %c", b, expectedRecordHeaderTypes[i])))

				// Don't increment i, retry reading the same expected field.
				i--

				continue
			}

			return nil, fmt.Errorf("%w: invalid record header line type %c expected %c",
				errclass.ErrCorruptData, b, expectedRecordHeaderTypes[i])
		}

		if err := expectChar(r.reader, ' '); err != nil {
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
		return fmt.Errorf("%w: read too many record header lines, offset: %d", errclass.ErrCorruptData, i)
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

	for range count {
		if err := expectChar(r.reader, markerRecordBins); err != nil {
			return nil, err
		}

		if err := expectChar(r.reader, ' '); err != nil {
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
	binType, err := r.reader.ReadByte()
	if err != nil {
		return err
	}

	if _, ok := binTypes[binType]; !ok {
		return fmt.Errorf("%w: invalid bin type %c", errclass.ErrCorruptData, binType)
	}

	base64Encoded, err := r.checkEncoded()
	if err != nil {
		return err
	}

	nameBytes, err := readUntilAnyEscaped(r.reader, delimsSpaceOrNewline)
	if err != nil {
		return err
	}

	name := string(nameBytes)

	// binTypeNil is a special case where the line ends after the bin name
	if binType == binTypeNil {
		if err := expectChar(r.reader, asbNewLine); err != nil {
			return err
		}

		bins[name] = nil

		return nil
	}

	if err := expectChar(r.reader, ' '); err != nil {
		return err
	}

	binVal, binErr := fetchBinValue(r, binType, base64Encoded)
	if binErr != nil {
		return binErr
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return err
	}

	bins[name] = binVal

	return nil
}

func (r *Decoder) checkEncoded() (bool, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return false, err
	}

	if b == ' ' {
		return true, nil
	}

	if b == '!' {
		if err := expectChar(r.reader, ' '); err != nil {
			return false, err
		}

		return false, nil
	}

	return false, fmt.Errorf("%w: invalid character %c, expected '!' or ' '", errclass.ErrCorruptData, b)
}

func fetchBinValue(r *Decoder, binType byte, base64Encoded bool) (any, error) {
	switch binType {
	case binTypeBool:
		return readBool(r.reader)
	case binTypeInt:
		return readSignedInt(r.reader, asbNewLine)
	case binTypeFloat:
		return readFloat(r.reader, asbNewLine)
	case binTypeString:
		return readStringSized(r.reader, ' ')
	case binTypeLDT:
		return nil, fmt.Errorf("%w: this backup contains LDTs, please restore it using an older restore tool"+
			" that supports LDTs", errclass.ErrUnsupported)
	case binTypeStringBase64:
		val, err := readBase64BytesSized(r.reader, ' ')
		if err != nil {
			return nil, err
		}

		return string(val), nil
	case binTypeGeoJSON:
		return readGeoJSON(r.reader, ' ')
	}

	if _, ok := bytesBinTypes[binType]; !ok {
		return nil, fmt.Errorf("%w: unexpected binType %d", errclass.ErrCorruptData, binType)
	}

	var (
		val []byte
		err error
	)

	if base64Encoded {
		val, err = readBase64BytesSized(r.reader, ' ')
	} else {
		val, err = readBytesSized(r.reader, ' ')
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
	default:
		return nil, fmt.Errorf("%w: invalid bytes to type binType %d", errclass.ErrCorruptData, binType)
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
func (r *Decoder) readUserKey() (any, error) {
	var res any

	keyTypeChar, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if _, ok := asbKeyTypes[keyTypeChar]; !ok {
		return nil, fmt.Errorf("%w: invalid key type %c", errclass.ErrCorruptData, keyTypeChar)
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
		return nil, fmt.Errorf("%w: invalid character %c, expected '!' or ' '", errclass.ErrCorruptData, keyTypeChar)
	}

	if !base64Encoded {
		if err := expectChar(r.reader, ' '); err != nil {
			return nil, err
		}
	}

	switch keyTypeChar {
	case keyTypeInt:
		keyVal, err := readSignedInt(r.reader, asbNewLine)
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeFloat:
		keyVal, err := readFloat(r.reader, asbNewLine)
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeString:
		keyVal, err := readStringSized(r.reader, ' ')
		if err != nil {
			return nil, err
		}

		res = keyVal

	case keyTypeStringBase64:
		keyVal, err := readBase64BytesSized(r.reader, ' ')
		if err != nil {
			return nil, err
		}

		res = string(keyVal)

	case keyTypeBytes:
		var cVal []byte
		if base64Encoded {
			cVal, err = readBase64BytesSized(r.reader, ' ')
		} else {
			cVal, err = readBytesSized(r.reader, ' ')
		}

		if err != nil {
			return nil, err
		}

		res = cVal

	default:
		// should never happen because of the previous check for membership in asbKeyTypes
		return nil, fmt.Errorf("%w: invalid key type %c", errclass.ErrCorruptData, keyTypeChar)
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return res, nil
}

func (r *Decoder) readBinCount() (uint16, error) {
	binCount, err := readUnsignedInt(r.reader, asbNewLine)
	if err != nil {
		return 0, err
	}

	if binCount > maxBinCount {
		return 0, fmt.Errorf("%w: invalid bin offset %d", errclass.ErrCorruptData, binCount)
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return 0, err
	}

	return uint16(binCount), nil
}

// readExpiration reads an expiration line from the asb file
// it expects that r has been advanced past the expiration line marker '+ t '
// NOTE: we don't check the expiration against any bounds because negative (large) expirations are valid
func (r *Decoder) readExpiration() (int64, error) {
	exp, err := readSignedInt(r.reader, asbNewLine)
	if err != nil {
		return 0, err
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return 0, err
	}

	if exp < 0 {
		return 0, fmt.Errorf("%w: invalid expiration time %d", errclass.ErrCorruptData, exp)
	}

	return exp, nil
}

func (r *Decoder) readGeneration() (uint32, error) {
	gen, err := readUnsignedInt(r.reader, asbNewLine)
	if err != nil {
		return 0, err
	}

	if gen > maxGeneration {
		return 0, fmt.Errorf("%w: invalid generation offset %d", errclass.ErrCorruptData, gen)
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return 0, err
	}

	return gen, nil
}

func (r *Decoder) readSet() (string, error) {
	set, err := readUntilEscaped(r.reader, asbNewLine)
	if err != nil {
		return "", err
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return "", err
	}

	return set, err
}

func (r *Decoder) readDigest() ([]byte, error) {
	digest, err := readBase64BytesDelimited(r.reader, asbNewLine)
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return digest, nil
}

func (r *Decoder) skipToNextLine() error {
	// Read until newline, no escaping needed for skip.
	// Result is discarded - we just need to advance the reader
	_, err := readUntilByte(r.reader, asbNewLine)
	if err != nil {
		return err
	}

	// Consume the newline.
	_, err = r.reader.ReadByte()

	return err
}

// ***** Helper Functions

func readBase64BytesDelimited(src *countingReader, delim byte) ([]byte, error) {
	encoded, err := readUntilByte(src, delim)
	if err != nil {
		return nil, err
	}

	return decodeBase64(encoded)
}

func readBase64BytesSized(src *countingReader, sizeDelim byte) ([]byte, error) {
	size, err := readUnsignedInt(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	if err := expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	return readBlockDecodeBase64(src, int64(size))
}

func readBlockDecodeBase64(src *countingReader, n int64) ([]byte, error) {
	data, err := readNBytes(src, n)
	if err != nil {
		return nil, err
	}

	return decodeBase64(data)
}

func decodeBase64(src []byte) ([]byte, error) {
	decodedLen := base64.StdEncoding.DecodedLen(len(src))
	buf := make([]byte, decodedLen)

	bw, err := base64.StdEncoding.Decode(buf, src)
	if err != nil {
		return nil, err
	}

	return buf[:bw], nil
}

func readStringSized(src *countingReader, sizeDelim byte) (string, error) {
	val, err := readBytesSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	return string(val), nil
}

func readBytesSized(src *countingReader, sizeDelim byte) ([]byte, error) {
	length, err := readUnsignedInt(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	if err := expectChar(src, sizeDelim); err != nil {
		return nil, err
	}

	return readNBytes(src, int64(length))
}

func readBool(src *countingReader) (bool, error) {
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
		return false, fmt.Errorf("%w: invalid boolean character %c", errclass.ErrCorruptData, b)
	}
}

func readFloat(src *countingReader, delim byte) (float64, error) {
	data, err := readUntil(src, delim)
	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(data, 64)
}

func readGeoJSON(src *countingReader, sizeDelim byte) (a.GeoJSONValue, error) {
	val, err := readStringSized(src, sizeDelim)
	if err != nil {
		return "", err
	}

	return a.NewGeoJSONValue(val), nil
}

func readHLL(src *countingReader, sizeDelim byte) (a.HLLValue, error) {
	data, err := readBytesSized(src, sizeDelim)
	if err != nil {
		return nil, err
	}

	return a.NewHLLValue(data), nil
}

func readUntilEscaped(src *countingReader, delim byte) (string, error) {
	result, err := readUntilByteEscaped(src, delim)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

func readUntil(src *countingReader, delim byte) (string, error) {
	result, err := readUntilByte(src, delim)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

// readUntilByte returns the bytes before delim, leaving delim unread so the
// caller can consume it (and so ReadByte updates line/column for a newline).
// Tokens larger than the reader's buffer are assembled across ReadSlice calls.
func readUntilByte(src *countingReader, delim byte) ([]byte, error) {
	var buf []byte

	for {
		slice, err := src.ReadSlice(delim)
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}

		// ReadSlice's buffer is invalidated by the next read; copy it out.
		// append of a nil slice is a no-op, which keeps NilAway happy.
		buf = append(buf, slice...)

		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}

		// err == nil: ReadSlice consumed delim as the last byte of this chunk.
		if len(buf) > 0 {
			buf = buf[:len(buf)-1]
		}

		if err := src.Reader.UnreadByte(); err != nil {
			return nil, err
		}

		src.tracker.noteBytes(buf)

		return buf, nil
	}
}

func readUntilByteEscaped(src *countingReader, delim byte) ([]byte, error) {
	var (
		buf []byte
		esc bool
	)

	for range maxTokenSize {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		if b == asbEscape && !esc {
			esc = true
			continue
		}

		if !esc && b == delim {
			return buf, src.UnreadByte()
		}

		esc = false

		buf = append(buf, b)
	}

	return nil, fmt.Errorf("%w: token larger than max size", errclass.ErrCorruptData)
}

func readUntilWhitespace(src *countingReader) ([]byte, error) {
	var buf []byte

	for range maxTokenSize {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		if b == ' ' || b == asbNewLine {
			return buf, src.UnreadByte()
		}

		buf = append(buf, b)
	}

	return nil, fmt.Errorf("%w: token larger than max size", errclass.ErrCorruptData)
}

func readUntilAnyEscaped(src *countingReader, delims []byte) ([]byte, error) {
	var (
		buf []byte
		esc bool
	)

	for range maxTokenSize {
		b, err := src.ReadByte()
		if err != nil {
			return nil, err
		}

		if b == asbEscape && !esc {
			esc = true
			continue
		}

		if !esc && bytes.IndexByte(delims, b) != -1 {
			return buf, src.UnreadByte()
		}

		esc = false

		buf = append(buf, b)
	}

	return nil, fmt.Errorf("%w: token larger than max size", errclass.ErrCorruptData)
}

func readNBytes(src *countingReader, n int64) ([]byte, error) {
	buf := make([]byte, n)

	_, err := io.ReadFull(src, buf)
	if err != nil {
		return nil, err
	}

	// Increase global offset.
	src.tracker.offset += uint64(n)

	// Update position tracker by counting newlines in the read data, only if we found at least one newline.
	if bytes.IndexByte(buf, asbNewLine) != -1 {
		newlineCount := bytes.Count(buf, []byte{asbNewLine})
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

func expectChar(src *countingReader, c byte) error {
	b, err := src.ReadByte()
	if err != nil {
		return err
	}

	if b == c {
		return nil
	}

	return fmt.Errorf("%w: invalid character, read %c, expected %c", errclass.ErrCorruptData, b, c)
}

func expectToken(src *countingReader, token string) error {
	data, err := readNBytes(src, int64(len(token)))
	if err != nil {
		return err
	}

	if string(data) != token {
		return fmt.Errorf("%w, read %s, expected %s", errInvalidToken, string(data), token)
	}

	return nil
}

func peek(src *countingReader) (byte, error) {
	b, err := src.ReadByte()
	if err != nil {
		return 0, err
	}

	err = src.UnreadByte()

	return b, err
}
