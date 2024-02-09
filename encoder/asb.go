package encoder

import (
	"backuplib/encoder/record"
	"backuplib/models"
	"errors"
	"fmt"
	"io"
	"strconv"
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
	return encodeSIndexToASB(sindex)
}

// TODO this file/the public api should think about marshaling in terms of the backup file format

// TODO use generics for things other than records
// func MarshalText(obj ASMarshaler) ([]byte, error) {
// 	return obj.MarshalText()
// }

func GetVersionText() []byte {
	versionString := strconv.FormatFloat(BackupFileVersion, 'f', -1, 64)
	return []byte(fmt.Sprintf("Version %s\n", versionString))
}

func GetNamespaceMetaText(namespace string) []byte {
	return []byte(fmt.Sprintf("# namespace %s\n", namespace))
}

func GetFirstMetaText() []byte {
	return []byte("# first-file\n")
}

// **** SINDEX ****

const (
	globalChar byte = '*'
	sindexChar byte = 'i'
)

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
func encodeSIndexToASB(sindex *models.SecondaryIndex) ([]byte, error) {
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
