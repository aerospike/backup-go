package encoder

import (
	"backuplib/encoder/record"
	"backuplib/models"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// PLANS ******
// each tyype that goes into the backup file ex: records, udf, sindex
// will get its own exported object. These will implement standard marshaling interfaces
// starting with encoding TextMarshaller's https://pkg.go.dev/encoding#TextMarshaler MarshalText() ([]byte, error)
// This package will export the Marshal generic function that will accept the exported types and call MarshalText on them (maybe)
// sub types like Key will not have exported methods/types but instead be parsed in the methods of their supertypes, ex for key: record

// To write serializer functions or wrapper types with marshal/unmarshal methods? that is the question
// func SerialzeRecord(r a.Record) {

// }

type MarshalType int

const (
	RECORD = iota
)

const (
	BackupFileVersion = 3.1
)

type ASBEncoder struct {
	writer    io.Writer
	first     bool
	namespace string
}

func NewASBEncoder(w io.Writer) (*ASBEncoder, error) {
	return &ASBEncoder{
		writer: w,
	}, nil
}

func (o *ASBEncoder) EncodeRecord(rec *models.Record) ([]byte, error) {
	// TODO this should take a pointer
	return record.NewRecord(*rec).MarshalText()
}

func (o *ASBEncoder) EncodeUDF(udf *models.UDF) ([]byte, error) {
	return nil, errors.New("UNIMPLEMENTED")
}

func (o *ASBEncoder) EncodeSIndex(sindex *models.SecondaryIndex) ([]byte, error) {
	return _SIndexToASB(sindex)
}

func GetVersionText() []byte {
	versionString := strconv.FormatFloat(BackupFileVersion, 'f', -1, 64)
	return []byte(fmt.Sprintf("Version %s\n", versionString))
}

func GetNamespaceMetaText(namespace string) []byte {
	return []byte(fmt.Sprintf("# namespace %s\n", escapeASBS(namespace)))
}

func GetFirstMetaText() []byte {
	return []byte("# first-file\n")
}

// **** RECORD ****

// line markers
const (
	recordMetaChar byte = '+'
	recordBinChar  byte = '-'
)

// constants
const (
	citrusLeafEpoch = 1262304000 // pulled from C client cf_clock.h
	asbTrue         = 'T'
	asbFalse        = 'F'
)

// function pointer for time.Now to allow for testing
var getTimeNow = time.Now

func recordToASB(r *models.Record) ([]byte, error) {
	var data []byte

	keyText, err := keyToASB(r.Key)
	if err != nil {
		return nil, err
	}
	data = append(data, keyText...)

	generationText := fmt.Sprintf("+ g %d\n", r.Generation)
	data = append(data, generationText...)

	exprTime := getExpirationTime(r.Expiration, getTimeNow().Unix())
	expirationText := fmt.Sprintf("+ t %d\n", exprTime)
	data = append(data, expirationText...)

	binCountText := fmt.Sprintf("+ b %d\n", len(r.Bins))
	data = append(data, binCountText...)

	binsText, err := binsToASB(r.Bins)
	if err != nil {
		return nil, err
	}
	data = append(data, binsText...)

	return data, nil
}

func binsToASB(bins a.BinMap) ([]byte, error) {
	var res []byte

	if len(bins) < 1 {
		return res, fmt.Errorf("ERR: empty binmap")
	}

	// NOTE golang's random order map iteration
	// means that any backup files that include
	// multi element bin maps will never be the same
	for k, v := range bins {
		binText, err := binToASB(k, v)
		if err != nil {
			return nil, err
		}

		res = append(res, binText...)
	}

	return res, nil
}

func binToASB(k string, v any) ([]byte, error) {
	var res []byte

	binName := escapeASBS(k)

	switch v := v.(type) {
	case bool:
		return []byte(fmt.Sprintf("- Z %s %c\n", binName, boolToASB(v))), nil
	case int64, int32, int16, int8, int:
		return []byte(fmt.Sprintf("- I %s %d\n", binName, v)), nil
	case float64:
		return []byte(fmt.Sprintf("- D %s %f\n", binName, v)), nil
	case string:
		return []byte(fmt.Sprintf("- S %s %d %s\n", binName, len(v), v)), nil
	case a.HLLValue:
		return []byte(fmt.Sprintf("- Y %s %d %s\n", binName, len(v), base64Encode(v))), nil
	case []byte:
		return []byte(fmt.Sprintf("- B %s %d %s\n", binName, len(v), base64Encode(v))), nil
	case map[any]any:
		return nil, errors.New("map bin not supported")
	case []any:
		return nil, errors.New("list bin not supported")
	case nil:
		return []byte(fmt.Sprintf("- N %s\n", binName)), nil
	default:
		return res, fmt.Errorf("unknown user key type: %T, key: %s", v, k)
	}
}

func boolToASB(b bool) byte {
	if b {
		return asbTrue
	}
	return asbFalse
}

func keyToASB(k *a.Key) ([]byte, error) {
	var data []byte

	if k == nil {
		return nil, errors.New("key is nil")
	}

	userKeyText, err := userKeyToASB(k.Value())
	if err != nil {
		return data, err
	}
	data = append(data, userKeyText...)

	namespaceText := fmt.Sprintf("+ n %s\n", k.Namespace())
	data = append(data, namespaceText...)

	b64Digest := base64Encode(k.Digest())
	digestText := fmt.Sprintf("+ d %s\n", b64Digest)
	data = append(data, digestText...)

	if k.SetName() != "" {
		setnameText := fmt.Sprintf("+ s %s\n", k.SetName())
		data = append(data, setnameText...)
	}

	return data, nil
}
func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func userKeyToASB(userKey a.Value) ([]byte, error) {
	var data []byte

	// user key is optional
	if userKey == nil {
		return nil, nil
	}

	val := userKey.GetObject()

	switch v := val.(type) {
	case int64, int32, int16, int8, int:
		data = []byte(fmt.Sprintf("+ k I %d\n", v))
	case float64:
		data = []byte(fmt.Sprintf("+ k D %f\n", v))
	case string:
		data = []byte(fmt.Sprintf("+ k S %d %s\n", len(v), v))
	case []byte:
		data = []byte(fmt.Sprintf("+ k B %d %s\n", len(v), base64Encode(v)))
	default:
		return nil, fmt.Errorf("invalid user key type: %T", v)
	}

	return data, nil
}

func getExpirationTime(ttl uint32, unix_now int64) uint32 {
	// math.MaxUint32 (-1) means never expire
	if ttl == math.MaxUint32 {
		return 0
	}

	timeSinceCE := unix_now - citrusLeafEpoch
	return uint32(timeSinceCE) + ttl
}

// **** SINDEX ****

// line markers
const (
	globalChar byte = '*'
	sindexChar byte = 'i'
)

// control characters
var asbEscapedChars = map[byte]struct{}{
	'\\': {},
	' ':  {},
	'\n': {},
}

func escapeASBS(s string) string {
	in := []byte(s)
	v := []byte{}
	for _, c := range in {
		if _, ok := asbEscapedChars[c]; ok {
			v = append(v, '\\')
		}
		v = append(v, c)
	}

	return string(v)
}

// TODO support escaped tokens
func _SIndexToASB(sindex *models.SecondaryIndex) ([]byte, error) {
	if sindex == nil {
		return nil, errors.New("sindex is nil")
	}

	// sindexes only ever use 1 path for now
	numPaths := 1

	v := fmt.Sprintf(
		"%c %c %s %s %s %c %d %s %c",
		globalChar,
		sindexChar,
		escapeASBS(sindex.Namespace),
		escapeASBS(sindex.Set),
		escapeASBS(sindex.Name),
		byte(sindex.IndexType),
		numPaths,
		escapeASBS(sindex.Path.BinName),
		byte(sindex.Path.BinType),
	)

	// TODO does this need to be base64 encoded?
	if sindex.Path.B64Context != "" {
		v = fmt.Sprintf("%s %s", v, sindex.Path.B64Context)
	}

	v += "\n"

	return []byte(v), nil
}
