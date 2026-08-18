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

package legacy_encoder

import (
	"bytes"
	"crypto/rand"
	"fmt"
	mRand "math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/assert"
)

func sortBinOutput(s string) []byte {
	var sorted sort.StringSlice = strings.Split(s, "\n")
	sorted.Sort()
	return []byte(strings.Join(sorted, "\n"))
}

func base64Encode(v []byte) []byte {
	encodedLen := base64.StdEncoding.EncodedLen(len(v))
	buf := make([]byte, encodedLen)
	base64.StdEncoding.Encode(buf, v)
	return buf
}

func Test_BinToASB(t *testing.T) {
	t.Parallel()
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
			name: "positive int32 bin",
			args: args{
				k: "binName",
				v: int32(123),
			},
			want: []byte("- I binName 123\n"),
		},
		{
			name: "positive int16 bin",
			args: args{
				k: "binName",
				v: int16(-123),
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
			want: fmt.Appendf(nil, "- Y binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("hello"))),
				base64.StdEncoding.EncodeToString([]byte("hello"))),
		},
		{
			name: "positive GeoJSON bin",
			args: args{
				k: "binName",
				v: a.GeoJSONValue(geoJSONStr),
			},
			want: fmt.Appendf(nil, "- G binName %d %s\n", len(geoJSONStr), geoJSONStr),
		},
		{
			name: "positive bytes bin",
			args: args{
				k: "binName",
				v: []byte("123"),
			},
			want: fmt.Appendf(nil, "- B binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123"))),
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
			want: fmt.Appendf(nil, "- M binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123"))),
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
			want: fmt.Appendf(nil, "- L binName %d %s\n",
				len(base64.StdEncoding.EncodeToString([]byte("123"))),
				base64.StdEncoding.EncodeToString([]byte("123"))),
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
			t.Parallel()
			dst := &bytes.Buffer{}
			n, err := BinToASB(tt.args.k, false, tt.args.v, dst)
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

func Test_BoolToASB(t *testing.T) {
	t.Parallel()
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
			want: []byte{boolTrueByte},
		},
		{
			name: "positive false",
			args: args{
				b: false,
			},
			want: []byte{boolFalseByte},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BoolToASB(tt.args.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_BinsToASB(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			dst := &bytes.Buffer{}
			n, err := BinsToASB(false, tt.args.bins, dst)
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

func Test_UserKeyToASB(t *testing.T) {
	t.Parallel()
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
				userKey: a.NewValue(123.456789),
			},
			want: []byte("+ k D 123.456789\n"),
		},
		{
			name: "positive negative float64 user key",
			args: args{
				userKey: a.NewValue(-123.456),
			},
			want: []byte("+ k D -123.456\n"),
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
			want: fmt.Appendf(nil, "+ k B %d %s\n", len(encVal), encVal),
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
			t.Parallel()
			dst := &bytes.Buffer{}
			n, err := UserKeyToASB(tt.args.userKey, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("UserKeyToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("UserKeyToASB() bytes written = %v, want %v", n, len(got))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UserKeyToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_KeyToASB(t *testing.T) {
	t.Parallel()
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
			want: fmt.Appendf(nil, "+ k I 1\n+ n ns\n+ d %s\n", base64Encode(NoSetKey.Digest())),
		},
		{
			name: "positive string key",
			args: args{
				k: stringKey,
			},
			want: fmt.Appendf(nil, "+ k S 5 hello\n+ n ns\n+ d %s\n+ s set\n", base64Encode(stringKey.Digest())),
		},
		{
			name: "positive escaped key",
			args: args{
				k: escKey,
			},
			want: fmt.Appendf(nil, "+ k S 5 hello\n+ n \\\\n\\ s\n+ d %s\n+ s set\\\n\n", base64Encode(escKey.Digest())),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dst := &bytes.Buffer{}
			n, err := KeyToASB(tt.args.k, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("KeyToASB() bytes written = %v, want %v", n, len(got))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KeyToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_RecordToASB(t *testing.T) {
	t.Parallel()
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
			want: fmt.Appendf(nil, "+ k S 4 1234\n+ n test\n+ d %s\n+ s demo\n+ g 1234\n+ t %d\n+ "+
				"b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(key.Digest()), recExpr),
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
			want: fmt.Appendf(nil, "+ k S 4 1234\n+ n test\\\n\n+ d %s\n+ s de\\ mo\n+ g 1234\n+ t %d\n+ "+
				"b 2\n- I bin1 0\n- S bin2 5 hello\n", base64Encode(escKey.Digest()), recExpr),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dst := &bytes.Buffer{}
			n, err := RecordToASB(false, tt.args.r, dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("RecordToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := dst.Bytes()
			if n != len(got) {
				t.Errorf("RecordToASB() bytes written = %v, want %v", n, len(got))
			}
			sortedGot := sortBinOutput(string(got))
			sortedWant := sortBinOutput(string(tt.want))
			if !reflect.DeepEqual(sortedGot, sortedWant) {
				t.Errorf("RecordToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_WriteRecordHeaderGeneration(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRecordHeaderGeneration(tt.args.generation, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordHeaderGeneration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRecordHeaderGeneration() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRecordHeaderGeneration() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRecordHeaderExpiration(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRecordHeaderExpiration(tt.args.expiration, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordHeaderExpiration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRecordHeaderExpiration() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRecordHeaderExpiration() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRecordHeaderBinCount(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRecordHeaderBinCount(tt.args.binCount, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordHeaderBinCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRecordHeaderBinCount() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRecordHeaderBinCount() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinInt(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinInt(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinInt() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinInt() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinFloat(t *testing.T) {
	t.Parallel()
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
			want:  len("- D binName 1234.5678\n"),
			wantW: "- D binName 1234.5678\n",
		},
		{
			name: "positive escaped",
			args: args{
				name: "b\nin\\Nam e",
				v:    1234.5678,
			},
			want:  len("- D b\\\nin\\\\Nam\\ e 1234.5678\n"),
			wantW: "- D b\\\nin\\\\Nam\\ e 1234.5678\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinFloat(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinFloat() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinFloat() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinString(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinString(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinString() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinString() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinBytes(t *testing.T) {
	t.Parallel()
	type args struct {
		compact bool
		name    string
		v       []byte
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
				compact: false,
				name:    "binName",
				v:       []byte("hello"),
			},
			want: len(fmt.Sprintf("- B binName %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello")))),
			wantW: fmt.Sprintf("- B binName %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello"))),
		},
		{
			name: "positive escaped",
			args: args{
				compact: false,
				name:    "b\nin\\Nam e",
				v:       []byte("hello"),
			},
			want: len(fmt.Sprintf("- B b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello")))),
			wantW: fmt.Sprintf("- B b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode([]byte("hello"))), base64Encode([]byte("hello"))),
		},
		{
			name: "positive compact simple",
			args: args{
				compact: true,
				name:    "binName",
				v:       []byte("hello"),
			},
			want:  len("- B! binName 5 hello\n"),
			wantW: "- B! binName 5 hello\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinBytes(tt.args.name, tt.args.compact, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinBytes() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinBytes() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinHLL(t *testing.T) {
	t.Parallel()
	type args struct {
		compact bool
		name    string
		v       a.HLLValue
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
				compact: false,
				name:    "binName",
				v:       a.HLLValue("hello"),
			},
			want: len(fmt.Sprintf("- Y binName %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello")))),
			wantW: fmt.Sprintf("- Y binName %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello"))),
		},
		{
			name: "positive escaped",
			args: args{
				compact: false,
				name:    "b\nin\\Nam e",
				v:       a.HLLValue("hello"),
			},
			want: len(fmt.Sprintf("- Y b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello")))),
			wantW: fmt.Sprintf("- Y b\\\nin\\\\Nam\\ e %d %s\n",
				len(base64Encode(a.HLLValue("hello"))), base64Encode(a.HLLValue("hello"))),
		},
		{
			name: "positive compact simple",
			args: args{
				compact: true,
				name:    "binName",
				v:       a.HLLValue("hello"),
			},
			want:  len("- Y! binName 5 hello\n"),
			wantW: "- Y! binName 5 hello\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinHLL(tt.args.name, tt.args.compact, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinHLL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinHLL() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinHLL() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinGeoJSON(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinGeoJSON(tt.args.name, tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinGeoJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinGeoJSON() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinGeoJSON() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteBinNil(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteBinNil(tt.args.name, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteBinNil() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteBinNil() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteBinNil() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRecordNamespace(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRecordNamespace(tt.args.namespace, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRecordNamespace() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRecordNamespace() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRecordDigest(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRecordDigest(tt.args.digest, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordDigest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRecordDigest() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRecordDigest() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRecordSet(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRecordSet(tt.args.setName, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRecordSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRecordSet() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRecordSet() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteUserKeyInt(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteUserKeyInt(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteUserKeyInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteUserKeyInt() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteUserKeyInt() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteUserKeyFloat(t *testing.T) {
	t.Parallel()
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
			want:  len("+ k D 1234.5678\n"),
			wantW: "+ k D 1234.5678\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteUserKeyFloat(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteUserKeyFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteUserKeyFloat() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteUserKeyFloat() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteUserKeyString(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteUserKeyString(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteUserKeyString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteUserKeyString() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteUserKeyString() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteUserKeyBytes(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteUserKeyBytes(tt.args.v, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteUserKeyBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteUserKeyBytes() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteUserKeyBytes() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func BenchmarkEncodeRecord(b *testing.B) {
	key := genKey()

	rec := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"IntBin":     123456789,
				"FloatBin":   98.6,
				"StringBin":  "This is a longer string to test buffer allocation",
				"BoolBin":    true,
				"NilBin":     nil,
				"SmallBlob":  []byte("small"),
				"LargeBlob":  bytes.Repeat([]byte("A"), 1024),
				"GeoJSONBin": a.GeoJSONValue(`{"type": "Point", "coordinates": [12.49, 41.89]}`),
				"MapBin": &a.RawBlobValue{
					ParticleType: particleType.MAP,
					Data:         []byte{0x81, 0xA2, 'i', 'd', 0x2A},
				},
				"ListBin": &a.RawBlobValue{
					ParticleType: particleType.LIST,
					Data:         []byte{0x93, 0x01, 0x02, 0x03},
				},
			},
			Generation: 5,
		},
		VoidTime: 3600,
	}

	b.ReportAllocs()
	buff := &bytes.Buffer{}
	for b.Loop() {
		buff.Reset()
		if _, err := RecordToASB(false, rec, buff); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBase64Encode(b *testing.B) {
	benchmarkSizes := []int{64, 256, 1024, 4096, 16384}

	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			data := make([]byte, size)
			_, err := rand.Read(data)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				encoded := base64Encode(data)
				if len(encoded) == 0 {
					b.Fatal("encoded data is empty")
				}
			}
		})
	}
}

func genKey() *a.Key {
	var key *a.Key
	var err error

	i := mRand.IntN(3)

	userKeys := []any{1, "string", []byte("bytes")}
	userKey := userKeys[i%len(userKeys)]

	switch k := userKey.(type) {
	case int:
		userKey = i
	case string:
		userKey = k + fmt.Sprint(i)
	case []byte:
		k = fmt.Appendf(k, "%d", i)
		userKey = k
	}
	key, err = a.NewKey("test", "demo", userKey)
	if err != nil {
		panic(err)
	}

	return key
}

func Test_BlobBinToASB(t *testing.T) {
	t.Parallel()
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
			want: fmt.Appendf(nil, "B binName %d %s\n", len([]byte("hello")), []byte("hello")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := BlobBinToASB(tt.args.val, tt.args.bytesType, tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BlobBinToASB() = %s, want %s", got, tt.want)
			}
		})
	}
}

func Test_WriteRawListBin(t *testing.T) {
	t.Parallel()
	data := []byte("hello")
	b64Data := base64.StdEncoding.EncodeToString(data)
	type args struct {
		compact bool
		cdt     *a.RawBlobValue
		name    string
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
				compact: false,
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
				compact: false,
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "b in\\Name\n",
			},
			want:  len(fmt.Sprintf("- L %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- L %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data),
		},
		{
			name: "positive compact simple",
			args: args{
				compact: true,
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "binName",
			},
			want:  len("- L! binName 5 hello\n"),
			wantW: "- L! binName 5 hello\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRawListBin(tt.args.cdt, tt.args.name, tt.args.compact, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRawListBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRawListBin() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRawListBin() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRawMapBin(t *testing.T) {
	t.Parallel()
	data := []byte("hello")
	b64Data := base64.StdEncoding.EncodeToString(data)
	type args struct {
		compact bool
		cdt     *a.RawBlobValue
		name    string
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
				compact: false,
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
				compact: false,
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "b in\\Name\n",
			},
			want:  len(fmt.Sprintf("- M %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data)),
			wantW: fmt.Sprintf("- M %s %d %s\n", "b\\ in\\\\Name\\\n", len(b64Data), b64Data),
		},
		{
			name: "positive compact simple",
			args: args{
				compact: true,
				cdt: &a.RawBlobValue{
					Data: data,
				},
				name: "binName",
			},
			want:  len("- M! binName 5 hello\n"),
			wantW: "- M! binName 5 hello\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRawMapBin(tt.args.cdt, tt.args.name, tt.args.compact, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRawMapBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRawMapBin() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRawMapBin() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func Test_WriteRawBlobBin(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			w := &bytes.Buffer{}
			got, err := WriteRawBlobBin(tt.args.cdt, tt.args.name, false, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteRawBlobBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WriteRawBlobBin() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteRawBlobBin() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}
