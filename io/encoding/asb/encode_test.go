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
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/require"
)

var testEncoderConfig = NewEncoderConfig("test", false, false)

func TestEncodeTokenUDF(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder[*models.Token](testEncoderConfig)

	token := &models.Token{
		Type: models.TokenTypeUDF,
		UDF: &models.UDF{
			Name:    "udf",
			UDFType: models.UDFTypeLUA,
			Content: []byte(base64.StdEncoding.EncodeToString([]byte("content"))),
		},
	}
	expected := appendUDFToASB(nil, token.UDF)

	actual, err := encoder.EncodeToken(token, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestEncodeTokenSIndex(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder[*models.Token](testEncoderConfig)

	token := &models.Token{
		Type: models.TokenTypeSIndex,
		SIndex: &models.SIndex{
			Namespace: "ns",
			Name:      "name",
			IndexType: models.BinSIndex,
			Path: models.SIndexPath{
				BinName: "bin",
				BinType: models.StringSIDataType,
			},
		},
	}

	expected := appendSIndexToASB(nil, token.SIndex)

	actual, err := encoder.EncodeToken(token, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestEncodeTokenInvalid(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder[*models.Token](testEncoderConfig)

	token := &models.Token{
		Type: models.TokenTypeInvalid,
	}

	token.Type = models.TokenTypeInvalid
	_, err := encoder.EncodeToken(token, nil)
	require.Error(t, err)
}

func TestEncodeRecord(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder[*models.Token](testEncoderConfig)

	var recExpr int64 = 10

	key, _ := a.NewKey("test", "demo", "1234")
	rec := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"bin1": 0,
			},
			Generation: 1234,
		},
		VoidTime: recExpr,
	}

	recTemplate := "+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t %d\n+ b 1\n- I bin1 0\n"
	expected := fmt.Sprintf(recTemplate, base64Encode(key.Digest()), recExpr)

	token := &models.Token{Type: models.TokenTypeRecord, Record: rec}
	actual, err := encoder.EncodeToken(token, nil)
	require.NoError(t, err)
	require.Equal(t, expected, string(actual))
}

func TestEncodeSIndex(t *testing.T) {
	t.Parallel()

	sindex := &models.SIndex{
		Namespace: "ns",
		Name:      "name",
		IndexType: models.BinSIndex,
		Path: models.SIndexPath{
			BinName: "bin",
			BinType: models.StringSIDataType,
		},
	}

	expected := []byte("* i ns  name N 1 bin S\n")
	got := appendSIndexToASB(nil, sindex)
	require.Len(t, expected, len(got))
	require.Equal(t, expected, got)
}

func TestGetHeaderFirst(t *testing.T) {
	t.Parallel()

	expected := "Version 3.1\n# namespace test\n# first-file\n"

	encoder := NewEncoder[*models.Token](testEncoderConfig)
	firstHeader := encoder.GetHeader(true)
	require.Equal(t, expected, string(firstHeader))

	secondExpected := "Version 3.1\n# namespace test\n"
	secondHeader := encoder.GetHeader(true)
	require.Equal(t, secondExpected, string(secondHeader))
}

func Test_appendEscapedDirect(t *testing.T) {
	t.Parallel()
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "positive no escape",
			args: args{
				s: "hello",
			},
			want: []byte("hello"),
		},
		{
			name: "positive escape",
			args: args{
				s: "h el\\lo\n",
			},
			want: []byte("h\\ el\\\\lo\\\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := appendEscapedDirect(nil, tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("appendEscapedDirect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test__SIndexToASB(t *testing.T) {
	t.Parallel()
	type args struct {
		sindex *models.SIndex
	}
	tests := []struct {
		args    args
		name    string
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive sindex no set or context",
			args: args{
				sindex: &models.SIndex{
					Namespace: "ns",
					Name:      "name",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName: "bin",
						BinType: models.StringSIDataType,
					},
				},
			},
			want:  len("* i ns  name N 1 bin S\n"),
			wantW: "* i ns  name N 1 bin S\n",
		},
		{
			name: "positive escaped sindex no context",
			args: args{
				sindex: &models.SIndex{
					Namespace: "n s",
					Name:      "name\n",
					Set:       "se\\t",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName: " bin",
						BinType: models.StringSIDataType,
					},
				},
			},
			want:  len("* i n\\ s se\\\\t name\\\n N 1 \\ bin S\n"),
			wantW: "* i n\\ s se\\\\t name\\\n N 1 \\ bin S\n",
		},
		{
			name: "positive sindex with set and context",
			args: args{
				sindex: &models.SIndex{
					Namespace: "ns",
					Name:      "name",
					Set:       "set",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName:    "bin",
						BinType:    models.StringSIDataType,
						B64Context: "context",
					},
				},
			},
			want:  len("* i ns set name N 1 bin S context\n"),
			wantW: "* i ns set name N 1 bin S context\n",
		},
		{
			name: "positive sindex with expression",
			args: args{
				sindex: &models.SIndex{
					Namespace: "ns",
					Name:      "name",
					Set:       "set",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName:    "bin",
						BinType:    models.StringSIDataType,
						B64Context: "context",
					},
					Expression: "expr",
				},
			},
			want:  len("* e ns set name N 1 bin S context expr\n"),
			wantW: "* e ns set name N 1 bin S context expr\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := appendSIndexToASB(nil, tt.args.sindex)
			if len(got) != tt.want {
				t.Errorf("appendSIndexToASB() = %v, want %v", len(got), tt.want)
			}
			if gotW := string(got); gotW != tt.wantW {
				t.Errorf("appendSIndexToASB() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_appendVersionText(t *testing.T) {
	t.Parallel()
	type args struct {
		asbVersion string
	}
	tests := []struct {
		name  string
		args  args
		wantW string
	}{
		{
			name: "positive simple",
			args: args{
				asbVersion: "3.2",
			},
			wantW: "Version 3.2\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := appendVersionText(nil, tt.args.asbVersion)
			if gotW := string(got); gotW != tt.wantW {
				t.Errorf("appendVersionText() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_appendNamespaceMetaText(t *testing.T) {
	t.Parallel()
	type args struct {
		namespace string
	}
	tests := []struct {
		name  string
		args  args
		wantW string
	}{
		{
			name: "positive simple",
			args: args{
				namespace: "test",
			},
			wantW: "# namespace test\n",
		},
		{
			name: "positive escaped",
			args: args{
				namespace: "t e\nst\\",
			},
			wantW: "# namespace t\\ e\\\nst\\\\\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := appendNamespaceMetaText(nil, tt.args.namespace)
			if gotW := string(got); gotW != tt.wantW {
				t.Errorf("appendNamespaceMetaMetaText() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_appendFirstMetaText(t *testing.T) {
	t.Parallel()
	got := appendFirstMetaText(nil)
	wantW := "# first-file\n"
	if gotW := string(got); gotW != wantW {
		t.Errorf("appendFirstMetaText() = %v, want %v", gotW, wantW)
	}
}

func sortBinOutput(s string) []byte {
	var sorted sort.StringSlice = strings.Split(s, "\n")
	sorted.Sort()
	return []byte(strings.Join(sorted, "\n"))
}

func Test_appendUDFToASB(t *testing.T) {
	t.Parallel()
	type args struct {
		udf *models.UDF
	}
	tests := []struct {
		args    args
		name    string
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				udf: &models.UDF{
					Name:    "hello.lua",
					Content: []byte("print('hello')"),
					UDFType: models.UDFTypeLUA,
				},
			},
			want:  len(fmt.Sprintf("* u L hello.lua %d %s\n", len("print('hello')"), "print('hello')")),
			wantW: fmt.Sprintf("* u L hello.lua %d %s\n", len("print('hello')"), "print('hello')"),
		},
		{
			name: "positive UDF name with escaped characters",
			args: args{
				udf: &models.UDF{
					Name:    "h\\e l\nlo.lua",
					Content: []byte("print('hi there')"),
					UDFType: models.UDFTypeLUA,
				},
			},
			want:  len(fmt.Sprintf("* u L h\\\\e\\ l\\\nlo.lua %d %s\n", len("print('hi there')"), "print('hi there')")),
			wantW: fmt.Sprintf("* u L h\\\\e\\ l\\\nlo.lua %d %s\n", len("print('hi there')"), "print('hi there')"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := appendUDFToASB(nil, tt.args.udf)
			if len(got) != tt.want {
				t.Errorf("appendUDFToASB() = %v, want %v", len(got), tt.want)
			}
			if gotW := string(got); gotW != tt.wantW {
				t.Errorf("appendUDFToASB() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func encoderTestKey(t *testing.T) *a.Key {
	t.Helper()

	key, err := a.NewKey("test", "demo", "key")
	require.NoError(t, err)

	return key
}

func encoderTestRecordPrefix(key *a.Key, generation uint32, voidTime int64, binCount int) string {
	return fmt.Sprintf("+ k S 3 key\n+ n test\n+ d %s\n+ s demo\n+ g %d\n+ t %d\n+ b %d\n",
		base64Encode(key.Digest()), generation, voidTime, binCount)
}

func encodeTestRecord(t *testing.T, compact bool, record *models.Record) ([]byte, error) {
	t.Helper()

	encoder := NewEncoder[*models.Token](NewEncoderConfig("test", compact, false))

	return encoder.appendRecord(nil, record)
}

func mustNewKey(t *testing.T, namespace, set string, userKey any) *a.Key {
	t.Helper()

	key, err := a.NewKey(namespace, set, userKey)
	require.NoError(t, err)

	return key
}

func TestAppendUserKey(t *testing.T) {
	t.Parallel()

	encVal := base64Encode([]byte("hello"))

	tests := []struct {
		name    string
		userKey a.Value
		want    []byte
		wantErr bool
	}{
		{
			name:    "int64 user key",
			userKey: a.NewValue(int64(123)),
			want:    []byte("+ k I 123\n"),
		},
		{
			name:    "negative int64 user key",
			userKey: a.NewValue(int64(-123)),
			want:    []byte("+ k I -123\n"),
		},
		{
			name:    "float64 user key",
			userKey: a.NewValue(123.456789),
			want:    []byte("+ k D 123.456789\n"),
		},
		{
			name:    "negative float64 user key",
			userKey: a.NewValue(-123.456),
			want:    []byte("+ k D -123.456\n"),
		},
		{
			name:    "string user key",
			userKey: a.NewValue("hello"),
			want:    []byte("+ k S 5 hello\n"),
		},
		{
			name:    "bytes user key",
			userKey: a.NewValue([]byte("hello")),
			want:    fmt.Appendf(nil, "+ k B %d %s\n", len(encVal), encVal),
		},
		{
			name:    "unknown user key",
			userKey: a.NewValue(true),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appendUserKey(nil, tt.userKey)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAppendRecordKey(t *testing.T) {
	t.Parallel()

	noSetKey, err := a.NewKey("ns", "", 1)
	require.NoError(t, err)
	stringKey, err := a.NewKey("ns", "set", "hello")
	require.NoError(t, err)
	escKey, err := a.NewKey("\\n s", "set\n", "hello")
	require.NoError(t, err)

	tests := []struct {
		name string
		key  *a.Key
		want []byte
	}{
		{
			name: "no set",
			key:  noSetKey,
			want: fmt.Appendf(nil, "+ k I 1\n+ n ns\n+ d %s\n", base64Encode(noSetKey.Digest())),
		},
		{
			name: "string user key",
			key:  stringKey,
			want: fmt.Appendf(nil, "+ k S 5 hello\n+ n ns\n+ d %s\n+ s set\n", base64Encode(stringKey.Digest())),
		},
		{
			name: "escaped namespace and set",
			key:  escKey,
			want: fmt.Appendf(nil, "+ k S 5 hello\n+ n \\\\n\\ s\n+ d %s\n+ s set\\\n\n", base64Encode(escKey.Digest())),
		},
	}

	encoder := NewEncoder[*models.Token](NewEncoderConfig("test", false, false))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := encoder.appendRecordKey(nil, tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAppendRecord(t *testing.T) {
	t.Parallel()

	var recExpr int64 = 10
	key, err := a.NewKey("test", "demo", "1234")
	require.NoError(t, err)
	escKey, err := a.NewKey("test\n", "de mo", "1234")
	require.NoError(t, err)

	tests := []struct {
		name   string
		record *models.Record
		want   []byte
	}{
		{
			name: "simple record",
			record: &models.Record{
				Record: &a.Record{
					Key: key,
					Bins: a.BinMap{
						"bin1": 0,
						"bin2": "hello",
					},
					Generation: 1234,
				},
				VoidTime: recExpr,
			},
			want: fmt.Appendf(nil, "+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t %d\n+ "+
				"b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(key.Digest()), recExpr),
		},
		{
			name: "escaped key metadata",
			record: &models.Record{
				Record: &a.Record{
					Key: escKey,
					Bins: a.BinMap{
						"bin1": 0,
						"bin2": "hello",
					},
					Generation: 1234,
				},
				VoidTime: recExpr,
			},
			want: fmt.Appendf(nil, "+ k S 4 1234\n+ n test\\\n\n+ d %s\n+ s de\\ mo\n+ g 1234\n+ t %d\n+ "+
				"b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(escKey.Digest()), recExpr),
		},
		{
			name: "zero bins",
			record: &models.Record{
				Record: &a.Record{
					Key:        key,
					Bins:       a.BinMap{},
					Generation: 5,
				},
				VoidTime: 99,
			},
			want: fmt.Appendf(nil, "+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 5\n+ t 99\n+ b 0\n",
				base64Encode(key.Digest())),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := encodeTestRecord(t, false, tt.record)
			require.NoError(t, err)
			if len(tt.record.Bins) <= 1 {
				require.Equal(t, tt.want, got)
				return
			}

			require.Equal(t, sortBinOutput(string(tt.want)), sortBinOutput(string(got)))
		})
	}
}

func TestAppendRecordBins(t *testing.T) {
	t.Parallel()

	geoJSONStr := `{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [0,0]]]}`
	key := encoderTestKey(t)
	prefix := encoderTestRecordPrefix(key, 1, 2, 1)

	tests := []struct {
		name    string
		binName string
		binVal  any
		want    []byte
		wantErr bool
	}{
		{name: "nil bin", binName: "binName", binVal: nil, want: []byte("- N binName\n")},
		{name: "escaped bin name", binName: "b\nin Nam\\e", binVal: nil, want: []byte("- N b\\\nin\\ Nam\\\\e\n")},
		{name: "bool true", binName: "binName", binVal: true, want: []byte("- Z binName T\n")},
		{name: "bool false", binName: "binName", binVal: false, want: []byte("- Z binName F\n")},
		{name: "int64 bin", binName: "binName", binVal: int64(123), want: []byte("- I binName 123\n")},
		{name: "negative int64 bin", binName: "binName", binVal: int64(-123), want: []byte("- I binName -123\n")},
		{name: "int32 bin", binName: "binName", binVal: int32(123), want: []byte("- I binName 123\n")},
		{name: "int16 bin", binName: "binName", binVal: int16(-123), want: []byte("- I binName -123\n")},
		{name: "int8 bin", binName: "binName", binVal: int8(7), want: []byte("- I binName 7\n")},
		{name: "int bin", binName: "binName", binVal: 42, want: []byte("- I binName 42\n")},
		{name: "float bin", binName: "binName", binVal: 123.456, want: []byte("- D binName 123.456\n")},
		{
			name: "float scientific notation long", binName: "binName", binVal: 8.699637788021931e-151,
			want: []byte("- D binName 8.699637788021931e-151\n"),
		},
		{
			name: "float scientific notation short", binName: "binName", binVal: 2.000511e-212,
			want: []byte("- D binName 2.000511e-212\n"),
		},
		{
			name: "negative float scientific notation long", binName: "binName", binVal: -9.799243036278548e-17,
			want: []byte("- D binName -9.799243036278548e-17\n"),
		},
		{
			name: "negative float scientific notation short", binName: "binName", binVal: -2.490355e+26,
			want: []byte("- D binName -2.490355e+26\n"),
		},
		{name: "string bin", binName: "binName", binVal: "hello", want: []byte("- S binName 5 hello\n")},
		{
			name: "HLL bin", binName: "binName", binVal: a.HLLValue("hello"),
			want: fmt.Appendf(nil, "- Y binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("hello"))),
				base64.StdEncoding.EncodeToString([]byte("hello"))),
		},
		{
			name: "GeoJSON bin", binName: "binName", binVal: a.GeoJSONValue(geoJSONStr),
			want: fmt.Appendf(nil, "- G binName %d %s\n", len(geoJSONStr), geoJSONStr),
		},
		{
			name: "bytes bin", binName: "binName", binVal: []byte("123"),
			want: fmt.Appendf(nil, "- B binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123"))),
		},
		{
			name: "map raw blob bin", binName: "binName",
			binVal: &a.RawBlobValue{ParticleType: particleType.MAP, Data: []byte("123")},
			want: fmt.Appendf(nil, "- M binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123"))),
		},
		{
			name: "list raw blob bin", binName: "binName",
			binVal: &a.RawBlobValue{ParticleType: particleType.LIST, Data: []byte("123")},
			want: fmt.Appendf(nil, "- L binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123"))),
		},
		{
			name: "invalid raw blob particle type", binName: "binName",
			binVal: &a.RawBlobValue{ParticleType: particleType.NULL}, wantErr: true,
		},
		{name: "map bin", binName: "binName", binVal: map[any]any{}, wantErr: true},
		{name: "list bin", binName: "binName", binVal: []any{}, wantErr: true},
		{name: "unknown bin type", binName: "binName", binVal: struct{}{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			record := &models.Record{
				Record: &a.Record{
					Key:        key,
					Bins:       a.BinMap{tt.binName: tt.binVal},
					Generation: 1,
				},
				VoidTime: 2,
			}

			got, err := encodeTestRecord(t, false, record)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, append([]byte(nil), prefix...), got[:len(prefix)])
			require.Equal(t, tt.want, got[len(prefix):])
		})
	}
}

func TestAppendRecordCompactBins(t *testing.T) {
	t.Parallel()

	key := encoderTestKey(t)
	prefix := encoderTestRecordPrefix(key, 1, 2, 1)

	tests := []struct {
		name    string
		binName string
		binVal  any
		want    []byte
	}{
		{name: "compact bytes", binName: "binName", binVal: []byte("hello"), want: []byte("- B! binName 5 hello\n")},
		{name: "compact HLL", binName: "binName", binVal: a.HLLValue("hello"), want: []byte("- Y! binName 5 hello\n")},
		{
			name: "compact map", binName: "binName",
			binVal: &a.RawBlobValue{ParticleType: particleType.MAP, Data: []byte("hello")},
			want:   []byte("- M! binName 5 hello\n"),
		},
		{
			name: "compact list", binName: "binName",
			binVal: &a.RawBlobValue{ParticleType: particleType.LIST, Data: []byte("hello")},
			want:   []byte("- L! binName 5 hello\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			record := &models.Record{
				Record: &a.Record{
					Key:        key,
					Bins:       a.BinMap{tt.binName: tt.binVal},
					Generation: 1,
				},
				VoidTime: 2,
			}

			got, err := encodeTestRecord(t, true, record)
			require.NoError(t, err)
			require.Equal(t, append([]byte(nil), prefix...), got[:len(prefix)])
			require.Equal(t, tt.want, got[len(prefix):])
		})
	}
}

func TestRecordWithUserKeyTypes(t *testing.T) {
	t.Parallel()

	encVal := base64Encode([]byte("hello"))

	tests := []struct {
		name       string
		key        *a.Key
		generation uint32
		voidTime   int64
		wantPrefix string
	}{
		{
			name:       "int user key",
			key:        mustNewKey(t, "test", "demo", int64(123)),
			generation: 7,
			voidTime:   8,
			wantPrefix: "+ k I 123\n+ n test\n+ d %s\n+ s demo\n+ g 7\n+ t 8\n+ b 1\n- I only 1\n",
		},
		{
			name:       "bytes user key",
			key:        mustNewKey(t, "test", "demo", []byte("hello")),
			generation: 7,
			voidTime:   8,
			wantPrefix: fmt.Sprintf("+ k B %d %s\n+ n test\n+ d %%s\n+ s demo\n+ g 7\n+ t 8\n+ b 1\n- I only 1\n",
				len(encVal), encVal),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			record := &models.Record{
				Record: &a.Record{
					Key:        tt.key,
					Bins:       a.BinMap{"only": 1},
					Generation: tt.generation,
				},
				VoidTime: tt.voidTime,
			}

			got, err := encodeTestRecord(t, false, record)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf(tt.wantPrefix, base64Encode(tt.key.Digest())), string(got))
		})
	}
}

func TestGenerateFilename(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder[*models.Token](NewEncoderConfig("backup-ns", false, false))

	first := encoder.GenerateFilename("pre_", ".part")
	second := encoder.GenerateFilename("pre_", ".part")

	require.Equal(t, "pre_backup-ns_1.part.asb", first)
	require.Equal(t, "pre_backup-ns_2.part.asb", second)
}

func TestGetHeaderNonRecordFiles(t *testing.T) {
	t.Parallel()

	t.Run("default version", func(t *testing.T) {
		t.Parallel()

		encoder := NewEncoder[*models.Token](NewEncoderConfig("test", false, false))
		require.Equal(t, "Version 3.1\n# namespace test\n# first-file\n", string(encoder.GetHeader(false)))
		require.Equal(t, "Version 3.1\n# namespace test\n", string(encoder.GetHeader(false)))
	})

	t.Run("expression sindex version", func(t *testing.T) {
		t.Parallel()

		encoder := NewEncoder[*models.Token](NewEncoderConfig("test", false, true))
		require.Equal(t, "Version 3.2\n# namespace test\n# first-file\n", string(encoder.GetHeader(false)))
	})
}

func TestEncodeTokenUnknownType(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder[*models.Token](NewEncoderConfig("test", false, false))
	token := &models.Token{Type: models.TokenType(99)}

	_, err := encoder.EncodeToken(token, []byte("prefix"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid token type")
}

func TestEncodeTokenRecordWithPrefix(t *testing.T) {
	t.Parallel()

	key, err := a.NewKey("test", "demo", "1234")
	require.NoError(t, err)

	record := &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       a.BinMap{"bin1": 0},
			Generation: 1234,
		},
		VoidTime: 10,
	}
	expected := fmt.Sprintf("+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t 10\n+ b 1\n- I bin1 0\n",
		base64Encode(key.Digest()))

	encoder := NewEncoder[*models.Token](NewEncoderConfig("test", false, false))
	token := &models.Token{Type: models.TokenTypeRecord, Record: record}

	dst := []byte("existing:")
	got, encodeErr := encoder.EncodeToken(token, dst)
	require.NoError(t, encodeErr)
	require.Equal(t, append([]byte("existing:"), []byte(expected)...), got)
}

func TestMetadataCacheHit(t *testing.T) {
	t.Parallel()

	key, err := a.NewKey("cached-ns", "cached-set", "key")
	require.NoError(t, err)

	record := func(value int) *models.Record {
		return &models.Record{
			Record: &a.Record{
				Key:        key,
				Bins:       a.BinMap{"value": value},
				Generation: 1,
			},
			VoidTime: 2,
		}
	}

	encoder := NewEncoder[*models.Token](NewEncoderConfig("test", false, false))

	first, firstErr := encoder.appendRecord(nil, record(1))
	require.NoError(t, firstErr)

	second, secondErr := encoder.appendRecord(nil, record(2))
	require.NoError(t, secondErr)

	require.NotEqual(t, first, second)
	require.Equal(t, 1, bytes.Count(first, []byte("+ n cached-ns\n")))
	require.Equal(t, 1, bytes.Count(second, []byte("+ n cached-ns\n")))
}

func TestAppendUserKeyTypedValues(t *testing.T) {
	t.Parallel()

	encVal := base64Encode([]byte("hello"))

	tests := []struct {
		name    string
		userKey a.Value
		want    []byte
	}{
		{name: "IntegerValue", userKey: a.IntegerValue(42), want: []byte("+ k I 42\n")},
		{name: "LongValue", userKey: a.LongValue(99), want: []byte("+ k I 99\n")},
		{name: "FloatValue", userKey: a.FloatValue(12.5), want: []byte("+ k D 12.5\n")},
		{
			name: "BytesValue", userKey: a.BytesValue([]byte("hello")),
			want: fmt.Appendf(nil, "+ k B %d %s\n", len(encVal), encVal),
		},
		{name: "NullValue", userKey: a.NullValue{}, want: nil},
		{name: "int32 via NewValue object", userKey: a.NewValue(int32(7)), want: []byte("+ k I 7\n")},
		{name: "int16 via NewValue object", userKey: a.NewValue(int16(-4)), want: []byte("+ k I -4\n")},
		{name: "int8 via NewValue object", userKey: a.NewValue(int8(3)), want: []byte("+ k I 3\n")},
		{name: "int via NewValue object", userKey: a.NewValue(11), want: []byte("+ k I 11\n")},
		{name: "nil via NewValue object", userKey: a.NewValue(nil), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appendUserKey(nil, tt.userKey)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAppendUserKeyHelpers(t *testing.T) {
	t.Parallel()

	encVal := base64Encode([]byte("hello"))

	tests := []struct {
		name string
		got  []byte
		want []byte
	}{
		{name: "int", got: appendUserKeyInt(nil, 1234), want: []byte("+ k I 1234\n")},
		{name: "float", got: appendUserKeyFloat(nil, 1234.5678), want: []byte("+ k D 1234.5678\n")},
		{name: "string", got: appendUserKeyString(nil, "hello"), want: []byte("+ k S 5 hello\n")},
		{
			name: "bytes", got: appendUserKeyBytes(nil, []byte("hello")),
			want: fmt.Appendf(nil, "+ k B %d %s\n", len(encVal), encVal),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.got)
		})
	}
}

// BenchmarkIntToString compares different methods of converting integers to strings.
func BenchmarkIntToString(b *testing.B) {
	num := int64(12345678901234)

	// Benchmark strconv.FormatInt
	b.Run("strconv.FormatInt", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = strconv.FormatInt(num, 10)
		}
	})

	// Benchmark fmt.Sprintf
	b.Run("fmt.Sprintf", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%d", num)
		}
	})

	// Benchmark fmt.Fprintf with bytes.Buffer
	b.Run("fmt.Fprintf-Buffer", func(b *testing.B) {
		var buf bytes.Buffer
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			_, _ = fmt.Fprintf(&buf, "%d", num)
			_ = buf.String()
		}
	})

	// Benchmark fmt.Fprintf with pre-allocated bytes.Buffer
	b.Run("fmt.Fprintf-PreallocBuffer", func(b *testing.B) {
		var buf bytes.Buffer
		buf.Grow(20) // Pre-allocate space for the result
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			_, _ = fmt.Fprintf(&buf, "%d", num)
			_ = buf.String()
		}
	})

	// Add strconv.Itoa for comparison (common for regular ints)
	b.Run("strconv.Itoa", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = strconv.Itoa(int(num))
		}
	})
}

func TestPrecomputedHeaderLines(t *testing.T) {
	values := []uint32{0, 1, 9, 42, 99, 100, 101, 65535}

	for _, value := range values {
		t.Run("generation/"+strconv.FormatUint(uint64(value), 10), func(t *testing.T) {
			want := fmt.Sprintf("+ g %d\n", value)
			for _, cache := range []bool{false, true} {
				got := string(appendGenerationLine(nil, value, cache))
				if got != want {
					t.Fatalf("appendGenerationLine(%d, cache=%v) = %q, want %q", value, cache, got, want)
				}
			}
		})
		t.Run("binCount/"+strconv.FormatUint(uint64(value), 10), func(t *testing.T) {
			want := fmt.Sprintf("+ b %d\n", value)
			for _, cache := range []bool{false, true} {
				got := string(appendBinCountLine(nil, value, cache))
				if got != want {
					t.Fatalf("appendBinCountLine(%d, cache=%v) = %q, want %q", value, cache, got, want)
				}
			}
		})
	}
}

func TestPrecomputedVoidTimeLine(t *testing.T) {
	t.Parallel()

	var buf [32]byte
	want := "+ t 0\n"

	gotCached := string(appendVoidTimeLine(nil, models.VoidTimeNeverExpire, true, buf[:0]))
	if gotCached != want {
		t.Fatalf("appendVoidTimeLine(0, cache=true) = %q, want %q", gotCached, want)
	}

	gotUncached := string(appendVoidTimeLine(nil, models.VoidTimeNeverExpire, false, buf[:0]))
	if gotUncached != want {
		t.Fatalf("appendVoidTimeLine(0, cache=false) = %q, want %q", gotUncached, want)
	}

	for _, voidTime := range []int64{1, 1712345678} {
		wantLine := fmt.Sprintf("+ t %d\n", voidTime)
		for _, cache := range []bool{false, true} {
			got := string(appendVoidTimeLine(nil, voidTime, cache, buf[:0]))
			if got != wantLine {
				t.Fatalf("appendVoidTimeLine(%d, cache=%v) = %q, want %q", voidTime, cache, got, wantLine)
			}
		}
	}
}

func BenchmarkPrecomputedGenerationLine(b *testing.B) {
	for _, cache := range []bool{false, true} {
		b.Run(fmt.Sprintf("cache=%v", cache), func(b *testing.B) {
			for _, value := range []uint32{1, 42, 101, 65535} {
				b.Run(strconv.FormatUint(uint64(value), 10), func(b *testing.B) {
					dst := make([]byte, 0, 16)
					b.ReportAllocs()
					for b.Loop() {
						dst = dst[:0]
						dst = appendGenerationLine(dst, value, cache)
					}
				})
			}
		})
	}
}

func BenchmarkPrecomputedBinCountLine(b *testing.B) {
	for _, cache := range []bool{false, true} {
		b.Run(fmt.Sprintf("cache=%v", cache), func(b *testing.B) {
			for _, value := range []uint32{1, 42, 101, 65535} {
				b.Run(strconv.FormatUint(uint64(value), 10), func(b *testing.B) {
					dst := make([]byte, 0, 16)
					b.ReportAllocs()
					for b.Loop() {
						dst = dst[:0]
						dst = appendBinCountLine(dst, value, cache)
					}
				})
			}
		})
	}
}

func newEncoderWithCache(cacheLine, cacheGen bool) *Encoder[*models.Token] {
	encoder := NewEncoder[*models.Token](testEncoderConfig)
	encoder.cacheLine = cacheLine
	encoder.cacheGen = cacheGen

	return encoder
}

func cacheBenchmarkRecord(key *a.Key, generation uint32, voidTime int64) *models.Record {
	return &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       a.BinMap{"bin1": int64(42), "bin2": "hello", "bin3": true},
			Generation: generation,
		},
		VoidTime: voidTime,
	}
}

func cacheBenchmarkToken(key *a.Key, generation uint32, voidTime int64) *models.Token {
	return &models.Token{
		Type:   models.TokenTypeRecord,
		Record: cacheBenchmarkRecord(key, generation, voidTime),
	}
}

// BenchmarkEncoderCache compares EncodeToken throughput for all cacheLine/cacheGen combinations.
func BenchmarkEncoderCache(b *testing.B) {
	key, err := a.NewKey("test", "demo", "benchmark-key")
	require.NoError(b, err)

	keyA, err := a.NewKey("namespace_a", "set_a", "key")
	require.NoError(b, err)

	keyB, err := a.NewKey("namespace_b", "set_b", "key")
	require.NoError(b, err)

	sameRecord := cacheBenchmarkToken(key, 42, 1712345678)
	alternatingTokens := []*models.Token{
		cacheBenchmarkToken(keyA, 1, 1712345678),
		cacheBenchmarkToken(keyB, 2, 1712345678),
	}

	varyingGenTokens := make([]*models.Token, 50)
	for i := range varyingGenTokens {
		varyingGenTokens[i] = cacheBenchmarkToken(key, uint32(i+1), 1712345678)
	}

	highGenRecord := cacheBenchmarkToken(key, 5000, 1712345678)

	warmOut, warmErr := newEncoderWithCache(false, false).EncodeToken(sameRecord, nil)
	require.NoError(b, warmErr)
	b.SetBytes(int64(len(warmOut)))

	type cacheConfig struct {
		name      string
		cacheLine bool
		cacheGen  bool
	}

	cacheConfigs := []cacheConfig{
		{name: "NoCache", cacheLine: false, cacheGen: false},
		{name: "CacheLine", cacheLine: true, cacheGen: false},
		{name: "CacheGen", cacheLine: false, cacheGen: true},
		{name: "CacheBoth", cacheLine: true, cacheGen: true},
	}

	for _, cfg := range cacheConfigs {
		b.Run("SameRecord/"+cfg.name, func(b *testing.B) {
			benchmarkEncoderCacheSameRecord(b, newEncoderWithCache(cfg.cacheLine, cfg.cacheGen), sameRecord)
		})
		b.Run("AlternatingMetadata/"+cfg.name, func(b *testing.B) {
			benchmarkEncoderCacheAlternatingMetadata(b, newEncoderWithCache(cfg.cacheLine, cfg.cacheGen), alternatingTokens)
		})
		b.Run("VaryingGeneration/"+cfg.name, func(b *testing.B) {
			benchmarkEncoderCacheVaryingGeneration(b, newEncoderWithCache(cfg.cacheLine, cfg.cacheGen), varyingGenTokens)
		})
		b.Run("HighGeneration/"+cfg.name, func(b *testing.B) {
			benchmarkEncoderCacheHighGeneration(b, newEncoderWithCache(cfg.cacheLine, cfg.cacheGen), highGenRecord)
		})
	}
}

// BenchmarkEncoderNeverExpireVoidTime measures EncodeToken with VoidTime 0 (never expire).
// cacheGen precomputes the "+ t 0\n" line alongside generation and bin-count lines.
func BenchmarkEncoderNeverExpireVoidTime(b *testing.B) {
	key, err := a.NewKey("test", "demo", "benchmark-key")
	require.NoError(b, err)

	neverExpireRecord := cacheBenchmarkToken(key, 42, models.VoidTimeNeverExpire)

	warmOut, warmErr := newEncoderWithCache(true, false).EncodeToken(neverExpireRecord, nil)
	require.NoError(b, warmErr)
	b.SetBytes(int64(len(warmOut)))

	for _, cacheGen := range []bool{false, true} {
		b.Run(fmt.Sprintf("cacheGen=%v", cacheGen), func(b *testing.B) {
			encoder := newEncoderWithCache(true, cacheGen)
			benchmarkEncoderCacheSameRecord(b, encoder, neverExpireRecord)
		})
	}
}

func BenchmarkAppendVoidTimeLine(b *testing.B) {
	var buf [32]byte
	for _, cache := range []bool{false, true} {
		b.Run(fmt.Sprintf("VoidTime0/cache=%v", cache), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				dst := appendVoidTimeLine(nil, models.VoidTimeNeverExpire, cache, buf[:0])
				if len(dst) == 0 {
					b.Fatal("empty output")
				}
			}
		})
	}
}

func benchmarkEncoderCacheSameRecord(b *testing.B, encoder *Encoder[*models.Token], token *models.Token) {
	b.Helper()
	b.ReportAllocs()

	out := make([]byte, 0, 4096)
	for b.Loop() {
		out = out[:0]

		var err error
		out, err = encoder.EncodeToken(token, out)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkEncoderCacheAlternatingMetadata(b *testing.B, encoder *Encoder[*models.Token], tokens []*models.Token) {
	b.Helper()
	b.ReportAllocs()

	out := make([]byte, 0, 4096)
	i := 0
	for b.Loop() {
		out = out[:0]

		var err error
		out, err = encoder.EncodeToken(tokens[i&1], out)
		if err != nil {
			b.Fatal(err)
		}

		i++
	}
}

func benchmarkEncoderCacheVaryingGeneration(b *testing.B, encoder *Encoder[*models.Token], tokens []*models.Token) {
	b.Helper()
	b.ReportAllocs()

	out := make([]byte, 0, 4096)
	i := 0
	for b.Loop() {
		out = out[:0]

		var err error
		out, err = encoder.EncodeToken(tokens[i%len(tokens)], out)
		if err != nil {
			b.Fatal(err)
		}

		i++
	}
}

func benchmarkEncoderCacheHighGeneration(b *testing.B, encoder *Encoder[*models.Token], token *models.Token) {
	b.Helper()
	b.ReportAllocs()

	out := make([]byte, 0, 4096)
	for b.Loop() {
		out = out[:0]

		var err error
		out, err = encoder.EncodeToken(token, out)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// base64Encode encodes the input bytes using base64 encoding.
func base64Encode(v []byte) []byte {
	encodedLen := base64.StdEncoding.EncodedLen(len(v))

	// Get a buffer from the pool
	buf := make([]byte, encodedLen)

	// Encode the data
	base64.StdEncoding.Encode(buf, v)

	// Return a slice that references the pooled buffer
	return buf
}
