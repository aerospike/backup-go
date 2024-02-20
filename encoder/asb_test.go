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

package encoder

import (
	"backuplib/models"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/suite"
)

type asbEncoderTestSuite struct {
	suite.Suite
}

func (suite *asbEncoderTestSuite) TestEncodeRecord() {
	dst := &strings.Builder{}
	encoder, err := NewASBEncoder(dst)
	if err != nil {
		suite.FailNow("unexpected error: %v", err)
	}

	now := time.Now()
	nowUnix := now.Unix()
	getTimeNow = func() time.Time { return now }
	defer func() { getTimeNow = time.Now }()
	recExpr := 10
	expExpr := (nowUnix - citrusLeafEpoch) + int64(recExpr)

	key, _ := a.NewKey("test", "demo", "1234")
	rec := &models.Record{
		Key: key,
		Bins: a.BinMap{
			"bin1": 0,
		},
		Generation: 1234,
		Expiration: uint32(recExpr),
	}

	expected := fmt.Sprintf("+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t %d\n+ b 1\n- I bin1 0\n", base64Encode(key.Digest()), expExpr)

	actual, err := encoder.EncodeRecord(rec)
	suite.Assert().NoError(err)
	actStr := string(actual)
	suite.Assert().Equal(expected, actStr)

	actual, err = encoder.EncodeRecord(nil)
	suite.Assert().Error(err)
	suite.Assert().Nil(actual)
}

func (suite *asbEncoderTestSuite) TestEncodeSIndex() {
	dst := &strings.Builder{}
	encoder, err := NewASBEncoder(dst)
	if err != nil {
		suite.FailNow("unexpected error: %v", err)
	}

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
	actual, err := encoder.EncodeSIndex(sindex)
	suite.Assert().NoError(err)
	suite.Assert().Equal(expected, actual)

	actual, err = encoder.EncodeSIndex(nil)
	suite.Assert().Error(err)
	suite.Assert().Nil(actual)
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
		want string
	}{
		{
			name: "positive no escape",
			args: args{
				s: "hello",
			},
			want: "hello",
		},
		{
			name: "positive escape",
			args: args{
				s: "h el\\lo\n",
			},
			want: "h\\ el\\\\lo\\\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeASBS(tt.args.s); !reflect.DeepEqual(got, tt.want) {
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
		name    string
		args    args
		want    []byte
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
			want: []byte("* i ns  name N 1 bin S\n"),
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
			want: []byte("* i n\\ s se\\\\t name\\\n N 1 \\ bin S\n"),
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
			want: []byte("* i ns set name N 1 bin S context\n"),
		},
		{
			name: "negative sindex is nil",
			args: args{
				sindex: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := _SIndexToASB(tt.args.sindex)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeSIndexToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeSIndexToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_binToASB(t *testing.T) {
	encVal := base64Encode(a.HLLValue("hello"))
	type args struct {
		k string
		v any
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
			want: []byte("- D binName 123.456000\n"),
		},
		{
			name: "positive negative float bin",
			args: args{
				k: "binName",
				v: -123.456,
			},
			want: []byte("- D binName -123.456000\n"),
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
			want: []byte(fmt.Sprintf("- Y binName %d %s\n", len(encVal), encVal)),
		},
		{
			name: "positive bytes bin",
			args: args{
				k: "binName",
				v: []byte("hello"),
			},
			want: []byte(fmt.Sprintf("- B binName %d %s\n", len(encVal), encVal)),
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
			got, err := binToASB(tt.args.k, tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeBinToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
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
		args args
		want byte
	}{
		{
			name: "positive true",
			args: args{
				b: true,
			},
			want: asbTrue,
		},
		{
			name: "positive false",
			args: args{
				b: false,
			},
			want: asbFalse,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boolToASB(tt.args.b); got != tt.want {
				t.Errorf("boolToASB() = %v, want %v", got, tt.want)
			}
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
			got, err := binsToASB(tt.args.bins)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeBinsToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
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
			name: "positive nil user key",
			args: args{
				userKey: nil,
			},
			want: nil,
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
			got, err := userKeyToASB(tt.args.userKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("userKeyToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
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
		{
			name: "negative key is nil",
			args: args{
				k: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := keyToASB(tt.args.k)
			if (err != nil) != tt.wantErr {
				t.Errorf("keyToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keyToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_getExpirationTime(t *testing.T) {
	type args struct {
		ttl      uint32
		unix_now int64
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "positive simple",
			args: args{
				ttl:      123,
				unix_now: citrusLeafEpoch,
			},
			want: 123,
		},
		{
			name: "positive never expire",
			args: args{
				ttl: math.MaxUint32,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getExpirationTime(tt.args.ttl, tt.args.unix_now); got != tt.want {
				t.Errorf("getExpirationTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_recordToASB(t *testing.T) {
	now := time.Now()
	nowUnix := now.Unix()
	getTimeNow = func() time.Time { return now }
	defer func() { getTimeNow = time.Now }()
	recExpr := 10
	expExpr := (nowUnix - citrusLeafEpoch) + int64(recExpr)

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
					Key: key,
					Bins: a.BinMap{
						"bin1": 0,
						"bin2": "hello",
					},
					Generation: 1234,
					Expiration: uint32(recExpr),
				},
			},
			want: []byte(fmt.Sprintf("+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t %d\n+ b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(key.Digest()), expExpr)),
		},
		{
			name: "positive escaped key",
			args: args{
				r: &models.Record{
					Key: escKey,
					Bins: a.BinMap{
						"bin1": 0,
						"bin2": "hello",
					},
					Generation: 1234,
					Expiration: uint32(recExpr),
				},
			},
			want: []byte(fmt.Sprintf("+ k S 4 1234\n+ n test\\\n\n+ d %s\n+ s de\\ mo\n+ g 1234\n+ t %d\n+ b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(escKey.Digest()), expExpr)),
		},
		{
			name: "negative record is nil",
			args: args{
				r: nil,
			},
			wantErr: true,
		},
		{
			name: "negative key is nil",
			args: args{
				r: &models.Record{
					Key: nil,
				},
			},
			wantErr: true,
		},
		{
			name: "negative bins is nil",
			args: args{
				r: &models.Record{
					Key:        key,
					Bins:       nil,
					Expiration: uint32(recExpr),
					Generation: 1234,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := recordToASB(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("recordToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sortedGot := sortBinOutput(string(got))
			sortedWant := sortBinOutput(string(tt.want))
			if !reflect.DeepEqual(sortedGot, sortedWant) {
				t.Errorf("recordToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestGetFirstMetaText(t *testing.T) {
	enc := &ASBEncoder{}
	tests := []struct {
		name string
		want []byte
	}{
		{
			name: "positive simple",
			want: []byte("# first-file\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := enc.GetFirstMetaText(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFirstMetaText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetNamespaceMetaText(t *testing.T) {
	enc := &ASBEncoder{}
	type args struct {
		namespace string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "positive simple",
			args: args{
				namespace: "ns",
			},
			want: []byte(fmt.Sprintf("# namespace %s\n", "ns")),
		},
		{
			name: "positive escaped",
			args: args{
				namespace: " n\ns\\",
			},
			want: []byte(fmt.Sprintf("# namespace %s\n", "\\ n\\\ns\\\\")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := enc.GetNamespaceMetaText(tt.args.namespace); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetNamespaceMetaText() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestGetVersionText(t *testing.T) {
	enc := &ASBEncoder{}
	tests := []struct {
		name string
		want []byte
	}{
		{
			name: "positive simple",
			want: []byte(fmt.Sprintf("Version %s\n", ASBFormatVersion)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := enc.GetVersionText(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetVersionText() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}
