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
	"encoding/base64"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	particleType "github.com/aerospike/aerospike-client-go/v7/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type asbEncoderTestSuite struct {
	suite.Suite
}

func (suite *asbEncoderTestSuite) TestEncodeTokenRecord() {
	encoder := NewEncoder("test", false)

	key, aerr := a.NewKey("test", "demo", "1234")
	if aerr != nil {
		suite.FailNow("unexpected error: %v", aerr)
	}

	token := &models.Token{
		Type: models.TokenTypeRecord,
		Record: &models.Record{
			Record: &a.Record{
				Key: key,
				Bins: a.BinMap{
					"bin1": 0,
				},
			},
		},
	}

	buff := &bytes.Buffer{}
	_, err := encoder.encodeRecord(token.Record, buff)
	suite.Assert().NoError(err)
	expected := bytes.Clone(buff.Bytes())

	actual, err := encoder.EncodeToken(token)
	suite.Assert().NoError(err)
	suite.Assert().Equal(expected, actual)
}

func (suite *asbEncoderTestSuite) TestEncodeTokenUDF() {
	encoder := NewEncoder("test", false)

	token := &models.Token{
		Type: models.TokenTypeUDF,
		UDF: &models.UDF{
			Name:    "udf",
			UDFType: models.UDFTypeLUA,
			Content: []byte(base64.StdEncoding.EncodeToString([]byte("content"))),
		},
	}
	buff := &bytes.Buffer{}
	_, err := encoder.encodeUDF(token.UDF, buff)
	suite.NoError(err)
	expected := buff.Bytes()

	actual, err := encoder.EncodeToken(token)
	suite.Assert().NoError(err)
	suite.Assert().Equal(expected, actual)
}

func (suite *asbEncoderTestSuite) TestEncodeTokenSIndex() {
	encoder := NewEncoder("test", false)

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

	buff := &bytes.Buffer{}
	_, err := encoder.encodeSIndex(token.SIndex, buff)
	suite.Assert().NoError(err)
	expected := buff.Bytes()

	actual, err := encoder.EncodeToken(token)
	suite.Assert().NoError(err)
	suite.Assert().Equal(expected, actual)
}

func (suite *asbEncoderTestSuite) TestEncodeTokenInvalid() {
	encoder := NewEncoder("test", false)

	token := &models.Token{
		Type: models.TokenTypeInvalid,
	}

	token.Type = models.TokenTypeInvalid
	actual, err := encoder.EncodeToken(token)
	suite.Assert().Error(err)
	suite.Assert().Nil(actual)
}

func (suite *asbEncoderTestSuite) TestEncodeRecord() {
	encoder := NewEncoder("test", false)

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

	buff := &bytes.Buffer{}
	n, err := encoder.encodeRecord(rec, buff)
	suite.Assert().NoError(err)
	actual := buff.Bytes()
	suite.Assert().Equal(len(actual), n)
	suite.Assert().Equal(expected, string(actual))
}

func (suite *asbEncoderTestSuite) TestEncodeSIndex() {
	encoder := NewEncoder("test", false)

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
	buff := &bytes.Buffer{}
	n, err := encoder.encodeSIndex(sindex, buff)
	suite.Assert().Equal(len(expected), n)
	suite.Assert().NoError(err)
	suite.Assert().Equal(expected, buff.Bytes())
}

func (suite *asbEncoderTestSuite) TestGetHeaderFirst() {
	expected := "Version 3.1\n# namespace test\n# first-file\n"

	encoder := NewEncoder("test", false)
	firstHeader := encoder.GetHeader()
	suite.Assert().Equal(expected, string(firstHeader))

	secondExpected := "Version 3.1\n# namespace test\n"
	secondHeader := encoder.GetHeader()
	suite.Assert().Equal(secondExpected, string(secondHeader))
}

func TestASBEncoderTestSuite(t *testing.T) {
	suite.Run(t, new(asbEncoderTestSuite))
}

func Test_escapeASBS(t *testing.T) {
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
			if got := escapeASB(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("escapeASB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test__SIndexToASB(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := sindexToASB(tt.args.sindex, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeSIndexToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("encodeSIndexToASB() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("encodeSIndexToASB() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_binToASB(t *testing.T) {
	geoJSONStr := `{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [0,0]]]}`
	type args struct {
		v any
		k string
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive nil bin",
			args: args{
				k: "binName",
				v: nil,
			},
			want: []byte("- N binName\n"),
		},
		{
			name: "positive escaped bin name",
			args: args{
				k: "b\nin Nam\\e",
				v: nil,
			},
			want: []byte("- N b\\\nin\\ Nam\\\\e\n"),
		},
		{
			name: "positive bool bin",
			args: args{
				k: "binName",
				v: true,
			},
			want: []byte("- Z binName T\n"),
		},
		{
			name: "positive int bin",
			args: args{
				k: "binName",
				v: int64(123),
			},
			want: []byte("- I binName 123\n"),
		},
		{
			name: "positive negative int bin",
			args: args{
				k: "binName",
				v: int64(-123),
			},
			want: []byte("- I binName -123\n"),
		},
		{
			name: "positive float bin",
			args: args{
				k: "binName",
				v: 123.456,
			},
			want: []byte("- D binName 123.456\n"),
		},
		{
			name: "positive float scientific notation long bin",
			args: args{
				k: "binName",
				v: 8.699637788021931e-151,
			},
			want: []byte("- D binName 8.699637788021931e-151\n"),
		},
		{
			name: "positive float scientific notation short bin",
			args: args{
				k: "binName",
				v: 2.000511e-212,
			},
			want: []byte("- D binName 2.000511e-212\n"),
		},
		{
			name: "negative float scientific notation long bin",
			args: args{
				k: "binName",
				v: -9.799243036278548e-17,
			},
			want: []byte("- D binName -9.799243036278548e-17\n"),
		},
		{
			name: "negative float scientific notation short bin",
			args: args{
				k: "binName",
				v: -2.490355e+26,
			},
			want: []byte("- D binName -2.490355e+26\n"),
		},
		{
			name: "positive negative float bin",
			args: args{
				k: "binName",
				v: -123.456,
			},
			want: []byte("- D binName -123.456\n"),
		},
		{
			name: "positive string bin",
			args: args{
				k: "binName",
				v: "hello",
			},
			want: []byte("- S binName 5 hello\n"),
		},
		{
			name: "positive HLL bin",
			args: args{
				k: "binName",
				v: a.HLLValue("hello"),
			},
			want: []byte(fmt.Sprintf("- Y binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("hello"))),
				base64.StdEncoding.EncodeToString([]byte("hello")))),
		},
		{
			name: "positive GeoJSON bin",
			args: args{
				k: "binName",
				v: a.GeoJSONValue(geoJSONStr),
			},
			want: []byte(fmt.Sprintf("- G binName %d %s\n", len(geoJSONStr), geoJSONStr)),
		},
		{
			name: "positive bytes bin",
			args: args{
				k: "binName",
				v: []byte("123"),
			},
			want: []byte(fmt.Sprintf("- B binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123")))),
		},
		{
			name: "positive map raw blob bin",
			args: args{
				k: "binName",
				v: &a.RawBlobValue{
					ParticleType: particleType.MAP,
					Data:         []byte("123"),
				},
			},
			want: []byte(fmt.Sprintf("- M binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123")))),
		},
		{
			name: "positive list raw blob bin",
			args: args{
				k: "binName",
				v: &a.RawBlobValue{
					ParticleType: particleType.LIST,
					Data:         []byte("123"),
				},
			},
			want: []byte(fmt.Sprintf("- L binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123")))),
		},
		{
			name: "negative invalid raw bin type",
			args: args{
				k: "binName",
				v: &a.RawBlobValue{
					ParticleType: particleType.NULL,
				},
			},
			wantErr: true,
		},
		{
			name: "negative map bin",
			args: args{
				k: "binName",
				v: map[any]any{},
			},
			wantErr: true,
		},
		{
			name: "negative list bin",
			args: args{
				k: "binName",
				v: []any{},
			},
			wantErr: true,
		},
		{
			name: "negative unknown bin",
			args: args{
				k: "binName",
				v: struct{}{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &bytes.Buffer{}
			n, err := binToASB(tt.args.k, false, tt.args.v, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeBinToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("encodeBinToASB() bytes written = %v, want %v", n, len(got))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeBinToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_boolToASB(t *testing.T) {
	type args struct {
		b bool
	}
	tests := []struct {
		name string
		want []byte
		args args
	}{
		{
			name: "positive true",
			args: args{
				b: true,
			},
			want: trueBytes,
		},
		{
			name: "positive false",
			args: args{
				b: false,
			},
			want: falseBytes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boolToASB(tt.args.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_binsToASB(t *testing.T) {
	type args struct {
		bins a.BinMap
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				bins: a.BinMap{
					"bin1": true,
					"bin2": int64(123),
				},
			},
			want: []byte("- Z bin1 T\n- I bin2 123\n"),
		},
		{
			name: "negative unknown bin",
			args: args{
				bins: a.BinMap{
					"bin1": struct{}{},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &bytes.Buffer{}
			n, err := binsToASB(false, tt.args.bins, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeBinsToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("encodeBinsToASB() bytes written = %v, want %v", n, len(got))
			}
			sortedGot := sortBinOutput(string(got))
			sortedWant := sortBinOutput(string(tt.want))
			if !reflect.DeepEqual(sortedGot, sortedWant) {
				t.Errorf("encodeBinsToASB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func sortBinOutput(s string) []byte {
	var sorted sort.StringSlice = strings.Split(s, "\n")
	sorted.Sort()
	return []byte(strings.Join(sorted, "\n"))
}

func Test_userKeyToASB(t *testing.T) {
	encVal := base64Encode([]byte("hello"))
	type args struct {
		userKey a.Value
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive int64 user key",
			args: args{
				userKey: a.NewValue(int64(123)),
			},
			want: []byte("+ k I 123\n"),
		},
		{
			name: "positive negative int64 user key",
			args: args{
				userKey: a.NewValue(int64(-123)),
			},
			want: []byte("+ k I -123\n"),
		},
		{
			name: "positive float64 user key",
			args: args{
				userKey: a.NewValue(123.456),
			},
			want: []byte("+ k D 123.456000\n"),
		},
		{
			name: "positive negative float64 user key",
			args: args{
				userKey: a.NewValue(-123.456),
			},
			want: []byte("+ k D -123.456000\n"),
		},
		{
			name: "positive string user key",
			args: args{
				userKey: a.NewValue("hello"),
			},
			want: []byte("+ k S 5 hello\n"),
		},
		{
			name: "positive bytes user key",
			args: args{
				userKey: a.NewValue([]byte("hello")),
			},
			want: []byte(fmt.Sprintf("+ k B %d %s\n", len(encVal), encVal)),
		},
		{
			name: "negative unknown user key",
			args: args{
				userKey: a.NewValue(true),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &bytes.Buffer{}
			n, err := userKeyToASB(tt.args.userKey, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("userKeyToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("userKeyToASB() bytes written = %v, want %v", n, len(got))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("userKeyToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_keyToASB(t *testing.T) {
	NoSetKey, _ := a.NewKey("ns", "", 1)
	stringKey, _ := a.NewKey("ns", "set", "hello")
	escKey, _ := a.NewKey("\\n s", "set\n", "hello")
	type args struct {
		k *a.Key
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive no set no user key",
			args: args{
				k: NoSetKey,
			},
			want: []byte(fmt.Sprintf("+ k I 1\n+ n ns\n+ d %s\n", base64Encode(NoSetKey.Digest()))),
		},
		{
			name: "positive string key",
			args: args{
				k: stringKey,
			},
			want: []byte(fmt.Sprintf("+ k S 5 hello\n+ n ns\n+ d %s\n+ s set\n", base64Encode(stringKey.Digest()))),
		},
		{
			name: "positive escaped key",
			args: args{
				k: escKey,
			},
			want: []byte(fmt.Sprintf("+ k S 5 hello\n+ n \\\\n\\ s\n+ d %s\n+ s set\\\n\n", base64Encode(escKey.Digest()))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &bytes.Buffer{}
			n, err := keyToASB(tt.args.k, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("keyToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("keyToASB() bytes written = %v, want %v", n, len(got))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keyToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_recordToASB(t *testing.T) {
	var recExpr int64 = 10
	key, _ := a.NewKey("test", "demo", "1234")
	escKey, _ := a.NewKey("test\n", "de mo", "1234")

	type args struct {
		r *models.Record
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				r: &models.Record{
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
			},
			want: []byte(fmt.Sprintf("+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t %d\n+ "+
				"b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(key.Digest()), recExpr)),
		},
		{
			name: "positive escaped key",
			args: args{
				r: &models.Record{
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
			},
			want: []byte(fmt.Sprintf("+ k S 4 1234\n+ n test\\\n\n+ d %s\n+ s de\\ mo\n+ g 1234\n+ t %d\n+ "+
				"b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(escKey.Digest()), recExpr)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &bytes.Buffer{}
			n, err := recordToASB(false, tt.args.r, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("recordToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("recordToASB() bytes written = %v, want %v", n, len(got))
			}
			sortedGot := sortBinOutput(string(got))
			sortedWant := sortBinOutput(string(tt.want))
			if !reflect.DeepEqual(sortedGot, sortedWant) {
				t.Errorf("recordToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_writeRecordHeaderGeneration(t *testing.T) {
	type args struct {
		generation uint32
	}
	tests := []struct {
		name    string
		wantW   string
		want    int
		args    args
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				generation: 1234,
			},
			want:  len("+ g 1234\n"),
			wantW: "+ g 1234\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRecordHeaderGeneration(tt.args.generation, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRecordHeaderGeneration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRecordHeaderGeneration() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRecordHeaderGeneration() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRecordHeaderExpiration(t *testing.T) {
	type args struct {
		expiration int64
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				expiration: 1234,
			},
			want:  len("+ t 1234\n"),
			wantW: "+ t 1234\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRecordHeaderExpiration(tt.args.expiration, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRecordHeaderExpiration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRecordHeaderExpiration() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRecordHeaderExpiration() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRecordHeaderBinCount(t *testing.T) {
	type args struct {
		binCount int
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				binCount: 1234,
			},
			want:  len("+ b 1234\n"),
			wantW: "+ b 1234\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRecordHeaderBinCount(tt.args.binCount, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRecordHeaderBinCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRecordHeaderBinCount() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRecordHeaderBinCount() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinInt(t *testing.T) {
	type args struct {
		name string
		v    int64
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				name: "binName",
				v:    1234,
			},
			want:  len("- I binName 1234\n"),
			wantW: "- I binName 1234\n",
		},
		{
			name: "positive simple",
			args: args{
				name: "b\nin\\Nam e",
				v:    1234,
			},
			want:  len("- I b\\\nin\\\\Nam\\ e 1234\n"),
			wantW: "- I b\\\nin\\\\Nam\\ e 1234\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinInt(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinInt() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinInt() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinFloat(t *testing.T) {
	type args struct {
		name string
		v    float64
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				name: "binName",
				v:    1234.5678,
			},
			want:  len("- D binName 1234.567800\n"),
			wantW: "- D binName 1234.567800\n",
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Nam e",
				v:    1234.5678,
			},
			want:  len("- D b\\\nin\\\\Nam\\ e 1234.567800\n"),
			wantW: "- D b\\\nin\\\\Nam\\ e 1234.567800\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinFloat(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinFloat() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinFloat() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinString(t *testing.T) {
	type args struct {
		name string
		v    string
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
				name: "binName",
				v:    "hello",
			},
			want:  len("- S binName 5 hello\n"),
			wantW: "- S binName 5 hello\n",
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Nam e",
				v:    "hello",
			},
			want:  len("- S b\\\nin\\\\Nam\\ e 5 hello\n"),
			wantW: "- S b\\\nin\\\\Nam\\ e 5 hello\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinString(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinString() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinString() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinBytes(t *testing.T) {
	type args struct {
		name string
		v    []byte
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				name: "binName",
				v:    []byte("hello"),
			},
			want: len(fmt.Sprintf("- B binName %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello")))),
			wantW: fmt.Sprintf("- B binName %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello"))),
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Nam e",
				v:    []byte("hello"),
			},
			want: len(fmt.Sprintf("- B b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello")))),
			wantW: fmt.Sprintf("- B b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinBytes(tt.args.name, false, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinBytes() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinBytes() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinHLL(t *testing.T) {
	type args struct {
		name string
		v    a.HLLValue
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				name: "binName",
				v:    a.HLLValue("hello"),
			},
			want: len(fmt.Sprintf("- Y binName %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello")))),
			wantW: fmt.Sprintf("- Y binName %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello"))),
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Nam e",
				v:    a.HLLValue("hello"),
			},
			want: len(fmt.Sprintf("- Y b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello")))),
			wantW: fmt.Sprintf("- Y b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinHLL(tt.args.name, false, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinHLL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinHLL() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinHLL() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinGeoJSON(t *testing.T) {
	type args struct {
		name string
		v    a.GeoJSONValue
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
				name: "binName",
				v:    a.GeoJSONValue(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
			},
			want: len(fmt.Sprintf("- G binName %d %s\n",
				len(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
				`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`)),
			wantW: fmt.Sprintf("- G binName %d %s\n",
				len(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
				`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Name ",
				v:    a.GeoJSONValue(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
			},
			want: len(fmt.Sprintf("- G b\\\nin\\\\Name\\  %d %s\n",
				len(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
				`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`)),
			wantW: fmt.Sprintf("- G b\\\nin\\\\Name\\  %d %s\n",
				len(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
				`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinGeoJSON(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinGeoJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinGeoJSON() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinGeoJSON() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeBinNil(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				name: "binName",
			},
			want:  len("- N binName\n"),
			wantW: "- N binName\n",
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Name ",
			},
			want:  len("- N b\\\nin\\\\Name\\ \n"),
			wantW: "- N b\\\nin\\\\Name\\ \n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeBinNil(tt.args.name, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeBinNil() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeBinNil() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeBinNil() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRecordNamespace(t *testing.T) {
	type args struct {
		namespace string
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				namespace: "ns",
			},
			want:  len(fmt.Sprintf("+ n %s\n", "ns")),
			wantW: fmt.Sprintf("+ n %s\n", "ns"),
		},
		{
			name: "positive escaped",
			args: args{
				namespace: " n\ns\\",
			},
			want:  len(fmt.Sprintf("+ n %s\n", "\\ n\\\ns\\\\")),
			wantW: fmt.Sprintf("+ n %s\n", "\\ n\\\ns\\\\"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRecordNamespace(tt.args.namespace, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRecordNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRecordNamespace() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRecordNamespace() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRecordDigest(t *testing.T) {
	type args struct {
		digest []byte
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				digest: []byte("hello"),
			},
			want:  len(fmt.Sprintf("+ d %s\n", base64Encode([]byte("hello")))),
			wantW: fmt.Sprintf("+ d %s\n", base64Encode([]byte("hello"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRecordDigest(tt.args.digest, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRecordDigest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRecordDigest() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRecordDigest() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRecordSet(t *testing.T) {
	type args struct {
		setName string
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				setName: "set",
			},
			want:  len(fmt.Sprintf("+ s %s\n", "set")),
			wantW: fmt.Sprintf("+ s %s\n", "set"),
		},
		{
			name: "positive escaped",
			args: args{
				setName: " s\net\\",
			},
			want:  len(fmt.Sprintf("+ s %s\n", "\\ s\\\net\\\\")),
			wantW: fmt.Sprintf("+ s %s\n", "\\ s\\\net\\\\"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRecordSet(tt.args.setName, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRecordSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRecordSet() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRecordSet() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeUserKeyInt(t *testing.T) {
	type args struct {
		v int64
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				v: 1234,
			},
			want:  len("+ k I 1234\n"),
			wantW: "+ k I 1234\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeUserKeyInt(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeUserKeyInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeUserKeyInt() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeUserKeyInt() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeUserKeyFloat(t *testing.T) {
	type args struct {
		v float64
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				v: 1234.5678,
			},
			want:  len("+ k D 1234.567800\n"),
			wantW: "+ k D 1234.567800\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeUserKeyFloat(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeUserKeyFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeUserKeyFloat() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeUserKeyFloat() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeUserKeyString(t *testing.T) {
	type args struct {
		v string
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				v: "hello",
			},
			want:  len("+ k S 5 hello\n"),
			wantW: "+ k S 5 hello\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeUserKeyString(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeUserKeyString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeUserKeyString() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeUserKeyString() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeUserKeyBytes(t *testing.T) {
	type args struct {
		v []byte
	}
	tests := []struct {
		name    string
		wantW   string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				v: []byte("hello"),
			},
			want:  len(fmt.Sprintf("+ k B %d %s\n", len(base64Encode([]byte("hello"))), base64Encode([]byte("hello")))),
			wantW: fmt.Sprintf("+ k B %d %s\n", len(base64Encode([]byte("hello"))), base64Encode([]byte("hello"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeUserKeyBytes(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeUserKeyBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeUserKeyBytes() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeUserKeyBytes() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

// **** Benchmarks ****

func BenchmarkEncodeRecord(b *testing.B) {
	output := &bytes.Buffer{}
	encoder := NewEncoder("test", false)

	key := genKey()
	rec := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"IntBin":     1,
				"FloatBin":   1.1,
				"StringBin":  "string",
				"BoolBin":    true,
				"BlobBin":    []byte("bytes"),
				"GeoJSONBin": a.GeoJSONValue(`{"type": "Polygon", "coordinates": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}`),
			},
			Generation: 1234,
		},
		VoidTime: 10,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buff := &bytes.Buffer{}
		_, _ = encoder.encodeRecord(rec, buff)
		output.Write(buff.Bytes())
	}
}

func genKey() *a.Key {
	var key *a.Key
	var err error

	i := rand.Intn(3)

	userKeys := []any{1, "string", []byte("bytes")}
	userKey := userKeys[i%len(userKeys)]

	switch k := userKey.(type) {
	case int:
		userKey = i
	case string:
		userKey = k + fmt.Sprint(i)
	case []byte:
		k = append(k, []byte(fmt.Sprint(i))...)
		userKey = k
	}
	key, err = a.NewKey("test", "demo", userKey)
	if err != nil {
		panic(err)
	}

	return key
}

func Test_writeVersionText(t *testing.T) {
	type args struct {
		asbVersion string
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				asbVersion: "3.2",
			},
			want:  len("Version 3.2\n"),
			wantW: "Version 3.2\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			writeVersionText(tt.args.asbVersion, w)
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeVersionText() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeNamespaceMetaText(t *testing.T) {
	type args struct {
		namespace string
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				namespace: "test",
			},
			want:  len("# namespace test\n"),
			wantW: "# namespace test\n",
		},
		{
			name: "positive escaped",
			args: args{
				namespace: "t e\nst\\",
			},
			want:  len("# namespace t\\ e\\\nst\\\\\n"),
			wantW: "# namespace t\\ e\\\nst\\\\\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			writeNamespaceMetaText(tt.args.namespace, w)
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeNamespaceMetaText() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeFirstMetaText(t *testing.T) {
	tests := []struct {
		name    string
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name:  "positive simple",
			want:  len("# first-file\n"),
			wantW: "# first-file\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			writeFirstMetaText(w)
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeFirstMetaText() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_blobBinToASB(t *testing.T) {
	type args struct {
		name      string
		val       []byte
		bytesType byte
	}
	tests := []struct {
		name string
		want []byte
		args args
	}{
		{
			name: "positive simple",
			args: args{
				val:       []byte("hello"),
				bytesType: 'B',
				name:      "binName",
			},
			want: []byte(fmt.Sprintf("B binName %d %s\n", len([]byte("hello")), []byte("hello"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := blobBinToASB(tt.args.val, tt.args.bytesType, tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("blobBinToASB() = %s, want %s", got, tt.want)
			}
		})
	}
}

func Test_writeRawListBin(t *testing.T) {
	data := []byte("hello")
	b64Data := base64.StdEncoding.EncodeToString(data)
	type args struct {
		cdt  *a.RawBlobValue
		name string
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
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "binName",
			},
			want:  len(fmt.Sprintf("- L %s %d %s\n", "binName", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- L %s %d %s\n", "binName", len(b64Data), b64Data),
		},
		{
			name: "positive escaped bin name",
			args: args{
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "b in\\Name\n",
			},
			want:  len(fmt.Sprintf("- L %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- L %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRawListBin(tt.args.cdt, tt.args.name, false, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRawListBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRawListBin() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRawListBin() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRawMapBin(t *testing.T) {
	data := []byte("hello")
	b64Data := base64.StdEncoding.EncodeToString(data)
	type args struct {
		cdt  *a.RawBlobValue
		name string
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
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "binName",
			},
			want:  len(fmt.Sprintf("- M %s %d %s\n", "binName", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- M %s %d %s\n", "binName", len(b64Data), b64Data),
		},
		{
			name: "positive escaped bin name",
			args: args{
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "b in\\Name\n",
			},
			want:  len(fmt.Sprintf("- M %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- M %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRawMapBin(tt.args.cdt, tt.args.name, false, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRawMapBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRawMapBin() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRawMapBin() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_writeRawBlobBin(t *testing.T) {
	data := []byte("hello")
	b64Data := base64.StdEncoding.EncodeToString(data)
	type args struct {
		cdt  *a.RawBlobValue
		name string
	}
	tests := []struct {
		args    args
		name    string
		wantW   string
		want    int
		wantErr bool
	}{
		{
			name: "positive map",
			args: args{
				cdt: &a.RawBlobValue{
					ParticleType: particleType.MAP,
					Data:         data,
				},
				name: "binName",
			},
			want:  len(fmt.Sprintf("- M %s %d %s\n", "binName", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- M %s %d %s\n", "binName", len(b64Data), b64Data),
		},
		{
			name: "positive list",
			args: args{
				cdt: &a.RawBlobValue{
					ParticleType: particleType.LIST,
					Data:         data,
				},
				name: "binName",
			},
			want:  len(fmt.Sprintf("- L %s %d %s\n", "binName", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- L %s %d %s\n", "binName", len(b64Data), b64Data),
		},
		{
			name: "negative invalid particle type",
			args: args{
				cdt: &a.RawBlobValue{
					ParticleType: particleType.NULL,
					Data:         data,
				},
				name: "binName",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := writeRawBlobBin(tt.args.cdt, tt.args.name, false, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeRawBlobBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("writeRawBlobBin() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("writeRawBlobBin() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_udfToASB(t *testing.T) {
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
			w := &bytes.Buffer{}
			got, err := udfToASB(tt.args.udf, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("udfToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("udfToASB() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("udfToASB() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}
