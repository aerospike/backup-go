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
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/assert"
)

const testFileName = "test_backup.asb"

func newTestCountingReader(s string) *countingReader {
	return newCountingReader(strings.NewReader(s), testFileName)
}

func TestASBReader_readHeader(t *testing.T) {
	t.Parallel()
	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		want    *header
		name    string
		wantErr bool
	}{
		{
			name: "positive simple",
			fields: fields{
				reader: newTestCountingReader("Version 3.1\n"),
			},
			want: &header{
				Version: "3.1",
			},
			wantErr: false,
		},
		{
			name: "negative missing line feed",
			fields: fields{
				reader: newTestCountingReader("Version 3.1"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space",
			fields: fields{
				reader: newTestCountingReader("Version3.1"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad version number",
			fields: fields{
				reader: newTestCountingReader("Version 31"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readHeader()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readHeader() got = %v, want = %v\n", got, tt.want)
			}
		})
	}
}

func TestASBReader_readMetadata(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		want    *metaData
		name    string
		wantErr bool
	}{
		{
			name: "positive escaped namespace",
			fields: fields{
				reader: newTestCountingReader("# namespace ns\\\n1\n# first-file\n"),
			},
			want: &metaData{
				Namespace: "ns\n1",
				First:     true,
			},
			wantErr: false,
		},
		{
			name: "positive no first-file",
			fields: fields{
				reader: newTestCountingReader("# namespace customers9\nd"),
			},
			want: &metaData{
				Namespace: "customers9",
			},
			wantErr: false,
		},
		{
			name: "positive no namespace",
			fields: fields{
				reader: newTestCountingReader("# first-file\na"),
			},
			want: &metaData{
				First: true,
			},
			wantErr: false,
		},
		{
			name: "negative empty metadata line",
			fields: fields{
				reader: newTestCountingReader("# "),
			},
			want:    nil,
			wantErr: true, // this will be the EOF error
		},
		{
			name: "negative missing space",
			fields: fields{
				reader: newTestCountingReader("#namespace hugerecords\n# first-file\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing second space",
			fields: fields{
				reader: newTestCountingReader("# namespace dergin3\n#first-file\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad token",
			fields: fields{
				reader: newTestCountingReader("# namespace tester\n# bad-token\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad namespace format (no new line)",
			fields: fields{
				reader: newTestCountingReader("# namespace ns1"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad first-file (no new line)",
			fields: fields{
				reader: newTestCountingReader("# first-file bad-data"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readMetadata()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readMetadata() got = %v, want = %v\n", got, tt.want)
			}
		})
	}
}

func TestASBReader_readSIndex(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}

	type test struct {
		fields  fields
		want    *models.SIndex
		name    string
		wantErr bool
	}

	tests := []test{
		{
			name: "positive bin numeric",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 N 1 bin1 N\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: models.BinSIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.NumericSIDataType,
				},
			},
			wantErr: false,
		},
		{
			name: "positive bin numeric no set",
			fields: fields{
				reader: newTestCountingReader(" userdata1  sindex1 N 1 bin1 N\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "",
				Name:      "sindex1",
				IndexType: models.BinSIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.NumericSIDataType,
				},
			},
			wantErr: false,
		},
		{
			name: "positive bin numeric no set with context",
			fields: fields{
				reader: newTestCountingReader(" userdata1  sindex1 N 1 bin1 N context\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "",
				Name:      "sindex1",
				IndexType: models.BinSIndex,
				Path: models.SIndexPath{
					BinName:    "bin1",
					BinType:    models.NumericSIDataType,
					B64Context: "context",
				},
			},
			wantErr: false,
		},
		{
			name: "positive ListElement string",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 L 1 bin1 S\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: models.ListElementSIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.StringSIDataType,
				},
			},
			wantErr: false,
		},
		{
			name: "positive mapKey geo2dsphere",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 K 1 bin1 G\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: models.MapKeySIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.GEO2DSphereSIDataType,
				},
			},
			wantErr: false,
		},
		{
			name: "positive mapValue blob",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V 1 bin1 B\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: models.MapValueSIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.BlobSIDataType,
				},
			},
			wantErr: false,
		},
		{
			name: "negative missing first space",
			fields: fields{
				reader: newTestCountingReader("userdata1 testSet1 sindex1 V 1 bin1 B\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after namespace",
			fields: fields{
				reader: newTestCountingReader(" userdata1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after set",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after name",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid index type",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 Z 1 bin1 B\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after index type",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing size",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V  bin1 B\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after size",
			fields: fields{
				reader: newTestCountingReader("userdata1 testSet1 sindex1 V 1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after path name",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V 1 bin1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid path type",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V 1 bin1 Z\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing new line",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V 1 bin1 B"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative 0 paths",
			fields: fields{
				reader: newTestCountingReader(" userdata1 testSet1 sindex1 V 0\n"),
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "positive random 48" {
				fmt.Println("test")
			}
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readSIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readSIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readSIndex() got = %v, want = %v\n", got, tt.want)
			}
		})
	}
}

func TestASBReader_readUDF(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}

	type test struct {
		fields  fields
		want    *models.UDF
		name    string
		wantErr bool
	}

	tests := []test{
		{
			name: "positive lua udf",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf 11 lua-content\n"),
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Name:    "lua-udf",
				Content: []byte("lua-content"),
			},
			wantErr: false,
		},
		{
			name: "positive escaped udf name",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf\\\n1 14 lua-content\\\n1\n"),
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Name:    "lua-udf\n1",
				Content: []byte("lua-content\\\n1"),
			},
			wantErr: false,
		},
		{
			name: "negative missing space after udf type",
			fields: fields{
				reader: newTestCountingReader(" Llua-udf 11 lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after udf name",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf11 lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing length",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf  lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after length",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf 11lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing content",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf 11 \n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing new line",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf 11 lua-content"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing starting space",
			fields: fields{
				reader: newTestCountingReader("L lua-udf 11 lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid udf type",
			fields: fields{
				reader: newTestCountingReader(" Z lua-udf 11 lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after UDF name",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf11 lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid length",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf notanint lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after length",
			fields: fields{
				reader: newTestCountingReader(" L lua-udf 11lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readUDF()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readUDF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readUDF() got = %v, want = %v\n", got, tt.want)
			}
		})
	}
}

func TestASBReader_readBin(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}

	type test struct {
		fields  fields
		want    map[string]any
		name    string
		wantErr bool
	}

	tests := []test{
		{
			name: "positive nil bin",
			fields: fields{
				reader: newTestCountingReader("N nil-bin\n"),
			},
			want: map[string]any{
				"nil-bin": nil,
			},
			wantErr: false,
		},
		{
			name: "negative nil bin",
			fields: fields{
				reader: newTestCountingReader("N nil-bin"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive bool bin",
			fields: fields{
				reader: newTestCountingReader("Z bool-bin T\n"),
			},
			want: map[string]any{
				"bool-bin": true,
			},
			wantErr: false,
		},
		{
			name: "negative bool bin",
			fields: fields{
				reader: newTestCountingReader("Z bool-bin X\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive float bin",
			fields: fields{
				reader: newTestCountingReader("D float-bin 1.1\n"),
			},
			want: map[string]any{
				"float-bin": 1.1,
			},
			wantErr: false,
		},
		{
			name: "positive float bin with inf",
			fields: fields{
				reader: newTestCountingReader("D float-bin inf\n"),
			},
			want: map[string]any{
				"float-bin": math.Inf(1),
			},
			wantErr: false,
		},
		{
			name: "negative float bin",
			fields: fields{
				reader: newTestCountingReader("D float-bin notafloat\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive int bin",
			fields: fields{
				reader: newTestCountingReader("I int-bin 1\n"),
			},
			want: map[string]any{
				"int-bin": int64(1),
			},
			wantErr: false,
		},
		{
			name: "positive int bin with negative value",
			fields: fields{
				reader: newTestCountingReader("I int-bin -1\n"),
			},
			want: map[string]any{
				"int-bin": int64(-1),
			},
			wantErr: false,
		},
		{
			name: "negative int bin",
			fields: fields{
				reader: newTestCountingReader("I int-bin notanint\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive string bin",
			fields: fields{
				reader: newTestCountingReader("S string-bin 6 string\n"),
			},
			want: map[string]any{
				"string-bin": "string",
			},
			wantErr: false,
		},
		{
			name: "positive string bin with inline delimiter",
			fields: fields{
				reader: newTestCountingReader("S string-bin 7 str\ning\n"),
			},
			want: map[string]any{
				"string-bin": "str\ning",
			},
			wantErr: false,
		},
		{
			name: "negative string bin",
			fields: fields{
				reader: newTestCountingReader("S string-bin 6 asdf\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive base64 string bin",
			fields: fields{
				reader: newTestCountingReader(
					fmt.Sprintf("X base64-str %d %s\n",
						base64.StdEncoding.EncodedLen(7),
						base64.StdEncoding.EncodeToString([]byte("str\ning"))),
				),
			},
			want: map[string]any{
				"base64-str": "str\ning",
			},
			wantErr: false,
		},
		{
			name: "negative base64 string bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("X base64-str %d %s\n", 7, "string!")),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive geoJSON bin",
			fields: fields{
				reader: newTestCountingReader("G geojson-bin 36 {\"type\":\"Point\",\"coordinates\":[1,2]}\n"),
			},
			want: map[string]any{
				"geojson-bin": a.NewGeoJSONValue("{\"type\":\"Point\",\"coordinates\":[1,2]}"),
			},
			wantErr: false,
		},
		{
			name: "negative geoJSON bin",
			fields: fields{
				reader: newTestCountingReader("G geojson-bin 100 {\"type\":\"Point\",\"coordinates\":[1,2]"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive blob bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"B blob-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"blob-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive compressed blob bin",
			fields: fields{
				reader: newTestCountingReader("B! blob-bin 5 123\n4\n"),
			},
			want: map[string]any{
				"blob-bin": []byte("123\n4"),
			},
			wantErr: false,
		},
		{
			name: "negative blob bin",
			fields: fields{
				reader: newTestCountingReader("B blob-bin 4123\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative compressed blob bin",
			fields: fields{
				reader: newTestCountingReader("B! blob-bin 123\n4"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive java bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"J java-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"java-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive java bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"J java-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"java-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive c# bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"C c#-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"c#-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive python bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"P python-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"python-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive ruby bytes",
			fields: fields{
				reader: newTestCountingReader(
					fmt.Sprintf("R ruby-bin %d %s\n",
						base64.StdEncoding.EncodedLen(4),
						base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"ruby-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive php bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"H php-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"php-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive erlang bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"E erlang-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"erlang-bin": []byte{0, 1, 2, '\n'},
			},
			wantErr: false,
		},
		{
			name: "positive HLL bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"Y hll-bin %d %s\n",
					base64.StdEncoding.EncodedLen(4),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2, '\n'})),
				),
			},
			want: map[string]any{
				"hll-bin": a.NewHLLValue([]byte{0, 1, 2, '\n'}),
			},
			wantErr: false,
		},
		{
			name: "negative HLL bytes",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"Y hll-bin %d %s\n", base64.StdEncoding.EncodedLen(100),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2})),
				),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive list bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("L list-bin %d %s\n",
					len(base64.StdEncoding.EncodeToString([]byte("123"))),
					base64.StdEncoding.EncodeToString([]byte("123")))),
			},
			want: map[string]any{
				"list-bin": a.NewRawBlobValue(particleType.LIST, []byte("123")),
			},
			wantErr: false,
		},
		{
			name: "positive plain text list bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("L! list-bin %d %s\n",
					len("123"), []byte("123"))),
			},
			want: map[string]any{
				"list-bin": a.NewRawBlobValue(particleType.LIST, []byte("123")),
			},
			wantErr: false,
		},
		{
			name: "negative list bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("L list-bin %d %s\n",
					500, "123")),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive map bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("M map-bin %d %s\n",
					len(base64.StdEncoding.EncodeToString([]byte("123"))),
					base64.StdEncoding.EncodeToString([]byte("123")))),
			},
			want: map[string]any{
				"map-bin": a.NewRawBlobValue(particleType.MAP, []byte("123")),
			},
			wantErr: false,
		},
		{
			name: "positive plain text map bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("M! map-bin %d %s\n",
					len("123"), "123")),
			},
			want: map[string]any{
				"map-bin": a.NewRawBlobValue(particleType.MAP, []byte("123")),
			},
			wantErr: false,
		},
		{
			name: "negative map bin",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("M map-bin %d %s\n",
					500, "abcd")),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "positive escaped bin name",
			fields: fields{
				reader: newTestCountingReader("S escaped-bin\\\n 6 string\n"),
			},
			want: map[string]any{
				"escaped-bin\n": "string",
			},
			wantErr: false,
		},
		{
			name: "negative missing space after bin type",
			fields: fields{
				reader: newTestCountingReader("Nnil-bin\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative bad end char after bin name",
			fields: fields{
				reader: newTestCountingReader("N nil-bin|"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative missing space after bin name",
			fields: fields{
				reader: newTestCountingReader("I int-bin\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative missing line feed after int bin value",
			fields: fields{
				reader: newTestCountingReader("I int-bin 1"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative LDT bin",
			fields: fields{
				reader: newTestCountingReader("U ldt-bin 1\n"),
			},
			want:    map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "positive random 10" {
				fmt.Println("test")
			}
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got := make(map[string]any)
			err := r.readBin(got)
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readBin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readBin() got = %v, want = %v\n", got, tt.want)
			}
		})
	}
}

func TestASBReader_readKey(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}

	type test struct {
		fields  fields
		want    any
		name    string
		wantErr bool
	}

	tests := []test{
		{
			name: "positive int key",
			fields: fields{
				reader: newTestCountingReader("I 1\n"),
			},
			want:    int64(1),
			wantErr: false,
		},
		{
			name: "positive float key",
			fields: fields{
				reader: newTestCountingReader("D 1.1\n"),
			},
			want:    1.1,
			wantErr: false,
		},
		{
			name: "positive string key",
			fields: fields{
				reader: newTestCountingReader("S 6 string\n"),
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "positive base64 string key",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"X %d %s\n",
					base64.StdEncoding.EncodedLen(6),
					base64.StdEncoding.EncodeToString([]byte("string"))),
				),
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "positive blob key",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"B %d %s\n", base64.StdEncoding.EncodedLen(3),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2})),
				),
			},
			want:    []byte{0, 1, 2},
			wantErr: false,
		},
		{
			name: "positive compressed blob key",
			fields: fields{
				reader: newTestCountingReader("B! 3 123\n"),
			},
			want:    []byte("123"),
			wantErr: false,
		},
		{
			name: "negative bad key type",
			fields: fields{
				reader: newTestCountingReader("Z 1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after key type",
			fields: fields{
				reader: newTestCountingReader("I1\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after !",
			fields: fields{
				reader: newTestCountingReader("B!3 123\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after int key value",
			fields: fields{
				reader: newTestCountingReader("I 1"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after float key value",
			fields: fields{
				reader: newTestCountingReader("D 1.1"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after string key value",
			fields: fields{
				reader: newTestCountingReader("S 6 string"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after base64 string key value",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"X %d %s",
					base64.StdEncoding.EncodedLen(6),
					base64.StdEncoding.EncodeToString([]byte("string"))),
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after blob key value",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"B %d %s",
					base64.StdEncoding.EncodedLen(3),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2})),
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after compressed blob key value",
			fields: fields{
				reader: newTestCountingReader("B! 3 123"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after string size",
			fields: fields{
				reader: newTestCountingReader("S 6string\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after base64 string size",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"X %d%s\n", base64.StdEncoding.EncodedLen(6),
					base64.StdEncoding.EncodeToString([]byte("string"))),
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after blob size",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf(
					"B %d%s\n",
					base64.StdEncoding.EncodedLen(3),
					base64.StdEncoding.EncodeToString([]byte{0, 1, 2})),
				),
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readUserKey()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readKey() got = %v, want = %v\n", got, tt.want)
			}
		})
	}
}

func TestASBReader_readRecord(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}

	type test struct {
		fields  fields
		want    *models.Record
		name    string
		wantErr bool
	}

	overMaxASBToken := strings.Repeat("a", maxTokenSize+1)

	digest := []byte("12345678901234567890")
	encodedDigest := base64.StdEncoding.EncodeToString(digest)

	intKey, err := a.NewKeyWithDigest("namespace1", "set1", int64(10), digest)
	if err != nil {
		panic(err)
	}

	keyNoUserVal, err := a.NewKeyWithDigest("namespace1", "set1", nil, digest)
	if err != nil {
		panic(err)
	}

	keyNoUserValOrSet, err := a.NewKeyWithDigest("namespace1", "", nil, digest)
	if err != nil {
		panic(err)
	}

	tests := []test{
		{
			name: "positive key and set",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want: &models.Record{
				Record: &a.Record{
					Key: intKey,
					Bins: map[string]any{
						"bin1": nil,
						"bin2": int64(2),
					},
					Generation: 10,
				},
				VoidTime: 10,
			},
			wantErr: false,
		},
		{
			name: "positive set and no key",
			fields: fields{
				reader: newTestCountingReader(
					"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want: &models.Record{
				Record: &a.Record{
					Key: keyNoUserVal,
					Bins: map[string]any{
						"bin1": nil,
						"bin2": int64(2),
					},
					Generation: 10,
				},
				VoidTime: 10,
			},
			wantErr: false,
		},
		{
			name: "positive no set and no key",
			fields: fields{
				reader: newTestCountingReader(
					"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want: &models.Record{
				Record: &a.Record{
					Key: keyNoUserValOrSet,
					Bins: map[string]any{
						"bin1": nil,
						"bin2": int64(2),
					},
					Generation: 10,
				},
				VoidTime: 10,
			},
			wantErr: false,
		},
		{
			name: "negative bad starting char",
			fields: fields{
				reader: newTestCountingReader(
					"x n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing starting char",
			fields: fields{
				reader: newTestCountingReader(
					"n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after starting char",
			fields: fields{
				reader: newTestCountingReader(
					"+n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid record line type",
			fields: fields{
				reader: newTestCountingReader(
					"+ Z namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after record line type",
			fields: fields{
				reader: newTestCountingReader(
					"+ kI 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing namespace",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing digest",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing generation",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing expiration",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing bin offset",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing bins",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad user key",
			fields: fields{
				reader: newTestCountingReader(
					"+ k P hehe\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad namespace",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n " + overMaxASBToken + "\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad digest",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + overMaxASBToken + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad set",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s " + overMaxASBToken + "\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad generation",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g " + "notanint" + "\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad expiration",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 999999999999999999999999999999999999\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad bin offset",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 9999999999999999999999999999999999\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readRecord()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.EqualValues(t, got, tt.want)
		})
	}
}

func TestASBReader_readBinCount(t *testing.T) {
	t.Parallel()

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		name    string
		want    uint16
		wantErr bool
	}{
		{
			name: "positive bin offset",
			fields: fields{
				reader: newTestCountingReader("2\n"),
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "negative bad bin offset",
			fields: fields{
				reader: newTestCountingReader("notanint\n"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative missing line feed after bin offset",
			fields: fields{
				reader: newTestCountingReader("2"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative bin offset less than 0",
			fields: fields{
				reader: newTestCountingReader("-1\n"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative bin offset greater than max",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("%d\n", maxBinCount+1)),
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readBinCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readBinCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ASBReader.readBinCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_readExpiration(t *testing.T) {
	t.Parallel()
	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		name    string
		want    int64
		wantErr bool
	}{
		{
			name: "positive expiration",
			fields: fields{
				reader: newTestCountingReader("10\n"),
			},
			want:    10,
			wantErr: false,
		},
		{
			name: "positive no expiration",
			fields: fields{
				reader: newTestCountingReader("0\n"),
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "negative bad expiration",
			fields: fields{
				reader: newTestCountingReader("notanint\n"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative missing line feed after expiration",
			fields: fields{
				reader: newTestCountingReader("10"),
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readExpiration()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readExpiration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ASBReader.readExpiration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_readGeneration(t *testing.T) {
	t.Parallel()
	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		name    string
		want    uint32
		wantErr bool
	}{
		{
			name: "positive generation",
			fields: fields{
				reader: newTestCountingReader("10\n"),
			},
			want:    10,
			wantErr: false,
		},
		{
			name: "negative bad generation",
			fields: fields{
				reader: newTestCountingReader("notanint\n"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative missing line feed after generation",
			fields: fields{
				reader: newTestCountingReader("10"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative generation less than 0",
			fields: fields{
				reader: newTestCountingReader("-1\n"),
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative generation greater than max",
			fields: fields{
				reader: newTestCountingReader(fmt.Sprintf("%d\n", maxGeneration+1)),
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readGeneration()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readGeneration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ASBReader.readGeneration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_readSet(t *testing.T) {
	t.Parallel()
	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "positive set",
			fields: fields{
				reader: newTestCountingReader("set1\n"),
			},
			want:    "set1",
			wantErr: false,
		},
		{
			name: "negative missing line feed after set",
			fields: fields{
				reader: newTestCountingReader("set1"),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "negative set too long",
			fields: fields{
				reader: newTestCountingReader(strings.Repeat("a", maxTokenSize+1) + "\n"),
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readSet()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ASBReader.readSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_readDigest(t *testing.T) {
	t.Parallel()
	digest := []byte("12345678901234567890")
	encodedDigest := base64.StdEncoding.EncodeToString(digest)

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name: "positive digest",
			fields: fields{
				reader: newTestCountingReader(encodedDigest + "\n"),
			},
			want:    digest,
			wantErr: false,
		},
		{
			name: "negative bad digest",
			fields: fields{
				reader: newTestCountingReader("notabase64string!\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after digest",
			fields: fields{
				reader: newTestCountingReader(encodedDigest),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readDigest()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readDigest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readDigest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readBase64BytesDelimited(t *testing.T) {
	t.Parallel()
	str := "string"
	encodedStr := base64.StdEncoding.EncodeToString([]byte(str))

	type args struct {
		src   *countingReader
		delim byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive base64 string",
			args: args{
				src: newTestCountingReader(encodedStr + "\n"),

				delim: '\n',
			},
			want:    []byte(str),
			wantErr: false,
		},
		{
			name: "negative bad base64 string",
			args: args{
				src: newTestCountingReader("notabase64string!\n"),

				delim: '\n',
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing delimiter after base64 string",
			args: args{
				src: newTestCountingReader(encodedStr),

				delim: '\n',
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readBase64BytesDelimited(tt.args.src, tt.args.delim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readBase64BytesDelimited() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readBase64BytesDelimited() = %v, want %v", got, tt.want)
			}
			returnBase64Buffer(got)
		})
	}
}

func Test_readBase64BytesSized(t *testing.T) {
	t.Parallel()
	str := "string"
	encodedStr := base64.StdEncoding.EncodeToString([]byte(str))

	type args struct {
		src       *countingReader
		sizeDelim byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive base64 string",
			args: args{
				src: newTestCountingReader(fmt.Sprintf("%d %s", len(encodedStr), encodedStr)),

				sizeDelim: ' ',
			},
			want:    []byte(str),
			wantErr: false,
		},
		{
			name: "negative bad size",
			args: args{
				src: newTestCountingReader("notanint " + encodedStr),

				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad base64 string",
			args: args{
				src:       newTestCountingReader(fmt.Sprintf("%d notabase64string!", len("notabase64string!"))),
				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after size",
			args: args{
				src:       newTestCountingReader(fmt.Sprintf("%d%s", len(encodedStr), encodedStr)),
				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readBase64BytesSized(tt.args.src, tt.args.sizeDelim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readBase64BytesSized() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readBase64BytesSized() = %v, want %v", got, tt.want)
			}
			returnBase64Buffer(got)
		})
	}
}

func Test_decodeBase64(t *testing.T) {
	t.Parallel()
	str := "string"
	encodedStr := base64.StdEncoding.EncodeToString([]byte(str))

	type args struct {
		src []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive base64 string",
			args: args{
				src: []byte(encodedStr),
			},
			want:    []byte(str),
			wantErr: false,
		},
		{
			name: "negative bad base64 string",
			args: args{
				src: []byte("notabase64string!"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _decodeBase64(tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("_decodeBase64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_decodeBase64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readStringSized(t *testing.T) {
	t.Parallel()
	type args struct {
		src       *countingReader
		sizeDelim byte
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "positive string",
			args: args{
				src: newTestCountingReader("6 string"),

				sizeDelim: ' ',
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "negative bad size",
			args: args{
				src: newTestCountingReader("notanint string"),

				sizeDelim: ' ',
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "negative missing size",
			args: args{
				src: newTestCountingReader("string"),

				sizeDelim: ' ',
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "negative missing space after size",
			args: args{
				src: newTestCountingReader("6string"),

				sizeDelim: ' ',
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "negative missing string",
			args: args{
				src: newTestCountingReader("6 "),

				sizeDelim: ' ',
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readStringSized(tt.args.src, tt.args.sizeDelim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("_readString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readBytesSized(t *testing.T) {
	t.Parallel()
	type args struct {
		src       *countingReader
		sizeDelim byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive bytes",
			args: args{
				src: newTestCountingReader("6 string"),

				sizeDelim: ' ',
			},
			want:    []byte("string"),
			wantErr: false,
		},
		{
			name: "negative bad size",
			args: args{
				src: newTestCountingReader("notanint string"),

				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing size",
			args: args{
				src: newTestCountingReader("string"),

				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after size",
			args: args{
				src: newTestCountingReader("6string"),

				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing bytes",
			args: args{
				src: newTestCountingReader("6 "),

				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readBytesSized(tt.args.src, tt.args.sizeDelim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readBytesSized() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readBytesSized() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readBool(t *testing.T) {
	t.Parallel()
	type args struct {
		src *countingReader
	}
	tests := []struct {
		args    args
		name    string
		want    bool
		wantErr bool
	}{
		{
			name: "positive true",
			args: args{
				src: newTestCountingReader("T"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "positive false",
			args: args{
				src: newTestCountingReader("F"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "negative bad bool char",
			args: args{
				src: newTestCountingReader("X"),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "negative missing bool",
			args: args{
				src: newTestCountingReader(""),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readBool(tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("_readBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readFloat(t *testing.T) {
	t.Parallel()
	type args struct {
		src   *countingReader
		delim byte
	}
	tests := []struct {
		name    string
		args    args
		want    float64
		wantErr bool
	}{
		{
			name: "positive float",
			args: args{
				src: newTestCountingReader("1.2345\n"),

				delim: '\n',
			},
			want:    1.2345,
			wantErr: false,
		},
		{
			name: "negative bad float",
			args: args{
				src: newTestCountingReader("notafloat\n"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative missing delimiter after float",
			args: args{
				src: newTestCountingReader("1.2345"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readFloat(tt.args.src, tt.args.delim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("_readFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readInteger(t *testing.T) {
	t.Parallel()
	type args struct {
		src   *countingReader
		delim byte
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "positive int",
			args: args{
				src: newTestCountingReader("12345\n"),

				delim: '\n',
			},
			want:    12345,
			wantErr: false,
		},
		{
			name: "negative bad int",
			args: args{
				src: newTestCountingReader("notanint\n"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative missing delimiter after int",
			args: args{
				src: newTestCountingReader("12345"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readInteger(tt.args.src, tt.args.delim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readInteger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("_readInteger() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readSize(t *testing.T) {
	t.Parallel()
	type args struct {
		src   *countingReader
		delim byte
	}
	tests := []struct {
		name    string
		args    args
		want    uint32
		wantErr bool
	}{
		{
			name: "positive size",
			args: args{
				src: newTestCountingReader("12345\n"),

				delim: '\n',
			},
			want:    12345,
			wantErr: false,
		},
		{
			name: "negative bad size",
			args: args{
				src: newTestCountingReader("notanint\n"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative missing delimiter after size",
			args: args{
				src: newTestCountingReader("12345"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative size less than 0",
			args: args{
				src: newTestCountingReader("-1\n"),

				delim: '\n',
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "negative size greater than max",
			args: args{
				src: newTestCountingReader(fmt.Sprintf("%d\n", math.MaxUint32+1)),

				delim: '\n',
			},
			want:    math.MaxUint32,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readSize(tt.args.src, tt.args.delim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("_readSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readUntil(t *testing.T) {
	t.Parallel()
	type args struct {
		src     *countingReader
		delim   byte
		escaped bool
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "positive read until",
			args: args{
				src: newTestCountingReader("string\n"),

				delim:   '\n',
				escaped: false,
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "positive read until escaped",
			args: args{
				src: newTestCountingReader("str\\\ning\n"),

				delim:   '\n',
				escaped: true,
			},
			want:    "str\ning",
			wantErr: false,
		},
		{
			name: "positive read until unescaped control chars",
			args: args{
				src: newTestCountingReader("str\\\ning\n"),

				delim:   '\n',
				escaped: false,
			},
			want:    "str\\",
			wantErr: false,
		},
		{
			name: "negative no delimiter",
			args: args{
				src: newTestCountingReader("string"),

				delim:   '\n',
				escaped: false,
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readUntil(tt.args.src, tt.args.delim, tt.args.escaped)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readUntil() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readUntil() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readUntilAny(t *testing.T) {
	t.Parallel()
	type args struct {
		src     *countingReader
		delims  []byte
		escaped bool
	}
	tests := []struct {
		name    string
		want    []byte
		args    args
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				src: newTestCountingReader("string\n"),

				delims:  []byte{'\n'},
				escaped: false,
			},
			want:    []byte("string"),
			wantErr: false,
		},
		{
			name: "positive escaped",
			args: args{
				src: newTestCountingReader("str\\ing\n"),

				delims:  []byte{'\n'},
				escaped: true,
			},
			want:    []byte("string"),
			wantErr: false,
		},
		{
			name: "positive escaped delimiter",
			args: args{
				src: newTestCountingReader("str\\\ning\n"),

				delims:  []byte{'\n'},
				escaped: true,
			},
			want:    []byte("str\ning"),
			wantErr: false,
		},
		{
			name: "positive multiple delimiters",
			args: args{
				src: newTestCountingReader("strHing\n"),

				delims:  []byte{'\n', 'H'},
				escaped: false,
			},
			want:    []byte("str"),
			wantErr: false,
		},
		{
			name: "positive multiple escaped delimiters",
			args: args{
				src: newTestCountingReader("str\\Hing\n"),

				delims:  []byte{'\n', 'H'},
				escaped: true,
			},
			want:    []byte("strHing"),
			wantErr: false,
		},
		{
			name: "positive unescaped delimiter mid input",
			args: args{
				src: newTestCountingReader("strHing\n"),

				delims:  []byte{'H'},
				escaped: false,
			},
			want:    []byte("str"),
			wantErr: false,
		},
		{
			name: "negative empty delimiter list",
			args: args{
				src: newTestCountingReader("string\n"),

				delims:  []byte{},
				escaped: false,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative no delimiter",
			args: args{
				src: newTestCountingReader("string"),

				delims:  []byte{'\n'},
				escaped: false,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative no delimiter escaped",
			args: args{
				src: newTestCountingReader("string\\\n"),

				delims:  []byte{'\n'},
				escaped: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative token too long",
			args: args{
				src: newTestCountingReader(strings.Repeat("a", maxTokenSize+1) + "\n"),

				delims:  []byte{'\n'},
				escaped: false,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative input empty",
			args: args{
				src: newTestCountingReader(""),

				delims:  []byte{'\n'},
				escaped: false,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readUntilAny(tt.args.src, tt.args.delims, tt.args.escaped)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readUntilAny() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readUntilAny() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readNBytes(t *testing.T) {
	type args struct {
		src *countingReader
		n   int64
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
				src: newTestCountingReader("string"),
				n:   6,
			},
			want:    []byte("string"),
			wantErr: false,
		},
		{
			name: "positive read 0 bytes",
			args: args{
				src: newTestCountingReader("string"),
				n:   0,
			},
			want:    []byte{},
			wantErr: false,
		},
		{
			name: "negative n greater than input (EOF)",
			args: args{
				src: newTestCountingReader("string"),
				n:   7,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := _readNBytes(tt.args.src, tt.args.n)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readNBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readNBytes() = %v, want %v", got, tt.want)
			}
			returnBigBuffer(got)
		})
	}
}

func Test_expectChar(t *testing.T) {
	t.Parallel()
	type args struct {
		src *countingReader
		c   byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "positive expected char",
			args: args{
				src: newTestCountingReader("string"),
				c:   's',
			},
			wantErr: false,
		},
		{
			name: "negative unexpected char",
			args: args{
				src: newTestCountingReader("string"),
				c:   'x',
			},
			wantErr: true,
		},
		{
			name: "negative EOF",
			args: args{
				src: newTestCountingReader(""),
				c:   'x',
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := _expectChar(tt.args.src, tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("_expectChar() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_expectAnyChar(t *testing.T) {
	t.Parallel()
	type args struct {
		src   *countingReader
		chars []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "positive expected char",
			args: args{
				src: newTestCountingReader("string"),

				chars: []byte{'s'},
			},
			wantErr: false,
		},
		{
			name: "positive multiple possible expected chars",
			args: args{
				src: newTestCountingReader("string"),

				chars: []byte{'t', 's'},
			},
			wantErr: false,
		},
		{
			name: "negative unexpected char",
			args: args{
				src: newTestCountingReader("string"),

				chars: []byte{'x'},
			},
			wantErr: true,
		},
		{
			name: "negative unexpected char in multiple possible expected chars",
			args: args{
				src: newTestCountingReader("string"),

				chars: []byte{'x', 'y'},
			},
			wantErr: true,
		},
		{
			name: "negative EOF",
			args: args{
				src: newTestCountingReader(""),

				chars: []byte{'x'},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := _expectAnyChar(tt.args.src, tt.args.chars); (err != nil) != tt.wantErr {
				t.Errorf("_expectAnyChar() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_expectToken(t *testing.T) {
	t.Parallel()
	type args struct {
		src   *countingReader
		token string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "positive expected token",
			args: args{
				src: newTestCountingReader("string"),

				token: "string",
			},
			wantErr: false,
		},
		{
			name: "negative unexpected token",
			args: args{
				src: newTestCountingReader("string"),

				token: "x",
			},
			wantErr: true,
		},
		{
			name: "negative EOF",
			args: args{
				src: newTestCountingReader(""),

				token: "x",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := _expectToken(tt.args.src, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("_expectToken() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_peek(t *testing.T) {
	t.Parallel()
	type args struct {
		src *countingReader
	}
	tests := []struct {
		args    args
		name    string
		want    byte
		wantErr bool
	}{
		{
			name: "positive peek",
			args: args{
				src: newTestCountingReader("string"),
			},
			want:    's',
			wantErr: false,
		},
		{
			name: "negative EOF",
			args: args{
				src: newTestCountingReader(""),
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _peek(tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("_peek() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("_peek() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_readBins(t *testing.T) {
	t.Parallel()
	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	type args struct {
		count uint16
	}
	tests := []struct {
		fields  fields
		want    a.BinMap
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "positive single bin",
			fields: fields{
				reader: newTestCountingReader("- I bi\\\nn1 20\n"),
			},
			args: args{
				count: 1,
			},
			want: a.BinMap{
				"bi\nn1": int64(20),
			},
			wantErr: false,
		},
		{
			name: "positive multiple bins",
			fields: fields{
				reader: newTestCountingReader("- I bin1 20\n- I bin2 30\n"),
			},
			args: args{
				count: 2,
			},
			want: a.BinMap{
				"bin1": int64(20),
				"bin2": int64(30),
			},
			wantErr: false,
		},
		{
			name: "positive 0 bins",
			fields: fields{
				reader: newTestCountingReader(""),
			},
			args: args{
				count: 0,
			},
			want:    a.BinMap{},
			wantErr: false,
		},
		{
			name: "negative missing bin line start character",
			fields: fields{
				reader: newTestCountingReader("I bin1 20\n"),
			},
			args: args{
				count: 1,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after start character",
			fields: fields{
				reader: newTestCountingReader("-I bin1 20\n"),
			},
			args: args{
				count: 1,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad bins",
			fields: fields{
				reader: newTestCountingReader("- I bin1 20\n- I bin2 notanint\n"),
			},
			args: args{
				count: 2,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readBins(tt.args.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readBins() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readBins() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readGeoJSON(t *testing.T) {
	t.Parallel()
	type args struct {
		src       *countingReader
		sizeDelim byte
	}
	tests := []struct {
		name    string
		args    args
		want    a.GeoJSONValue
		wantErr bool
	}{
		{
			name: "positive geojson",
			args: args{
				src: newTestCountingReader("36 {\"type\":\"Point\",\"coordinates\":[1,2]}"),

				sizeDelim: ' ',
			},
			want:    "{\"type\":\"Point\",\"coordinates\":[1,2]}",
			wantErr: false,
		},
		{
			name: "negative bad size",
			args: args{
				src: newTestCountingReader("notanint {\"type\":\"Point\",\"coordinates\":[1,2]}"),

				sizeDelim: ' ',
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readGeoJSON(tt.args.src, tt.args.sizeDelim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readGeoJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readGeoJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readHLL(t *testing.T) {
	t.Parallel()
	type args struct {
		src       *countingReader
		sizeDelim byte
	}
	tests := []struct {
		name    string
		args    args
		want    a.HLLValue
		wantErr bool
	}{
		{
			name: "positive hll",
			args: args{
				src: newTestCountingReader("6 string"),

				sizeDelim: ' ',
			},
			want:    a.NewHLLValue([]byte("string")),
			wantErr: false,
		},
		{
			name: "negative bad size",
			args: args{
				src: newTestCountingReader("notanint string"),

				sizeDelim: ' ',
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := _readHLL(tt.args.src, tt.args.sizeDelim)
			if (err != nil) != tt.wantErr {
				t.Errorf("_readHLL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_readHLL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_readGlobals(t *testing.T) {
	t.Parallel()
	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		want    any
		name    string
		wantErr bool
	}{
		{
			name: "positive sindex",
			fields: fields{
				reader: newTestCountingReader("* i userdata1 testSet1 sindex1 N 1 bin1 N\n"),
			},
			want: &models.SIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: models.BinSIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.NumericSIDataType,
				},
			},
			wantErr: false,
		},
		{
			name: "positive UDF",
			fields: fields{
				reader: newTestCountingReader("* u L lua-udf 11 lua-content\n"),
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Name:    "lua-udf",
				Content: []byte("lua-content"),
			},
			wantErr: false,
		},
		{
			name: "negative missing start char",
			fields: fields{
				reader: newTestCountingReader(" i userdata1 testSet1 sindex1 N 1 bin1 N\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after start char",
			fields: fields{
				reader: newTestCountingReader("*i userdata1 testSet1 sindex1 N 1 bin1 N\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad type",
			fields: fields{
				reader: newTestCountingReader("* x userdata1 testSet1 sindex1 N 1 bin1 N\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad sindex",
			fields: fields{
				reader: newTestCountingReader("* i userdata1 testSet1 sindex1 X 1 bin1 N\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad udf",
			fields: fields{
				reader: newTestCountingReader("* u X lua-udf 11 lua-content\n"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.readGlobals()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readGlobals() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.readGlobals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestASBReader_NextToken(t *testing.T) {
	t.Parallel()
	digest := []byte("12345678901234567890")
	encodedDigest := base64.StdEncoding.EncodeToString(digest)

	intKey, err := a.NewKeyWithDigest("namespace1", "set1", int64(10), digest)
	if err != nil {
		panic(err)
	}

	type fields struct {
		header   *header
		metaData *metaData
		reader   *countingReader
	}
	tests := []struct {
		fields  fields
		want    *models.Token
		name    string
		wantErr bool
	}{
		{
			name: "positive global line",
			fields: fields{
				reader: newTestCountingReader("* i userdata1 testSet1 sindex1 N 1 bin1 N\n"),
			},
			want: models.NewSIndexToken(&models.SIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: models.BinSIndex,
				Path: models.SIndexPath{
					BinName: "bin1",
					BinType: models.NumericSIDataType,
				},
			}, 42),
			wantErr: false,
		},
		{
			name: "positive record line",
			fields: fields{
				reader: newTestCountingReader(
					"+ k I 10\n" +
						"+ n namespace1\n" +
						"+ d " + encodedDigest + "\n" +
						"+ s set1\n" +
						"+ g 10\n" +
						"+ t 10\n" +
						"+ b 2\n" +
						"- N bin1\n" +
						"- I bin2 2\n",
				),
			},
			want: models.NewRecordToken(&models.Record{
				Record: &a.Record{
					Key: intKey,
					Bins: map[string]any{
						"bin1": nil,
						"bin2": int64(2),
					},
					Generation: 10,
				},
				VoidTime: 10,
			}, 106, nil),
		},
		{
			name: "negative EOF",
			fields: fields{
				reader: newTestCountingReader(""),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad record line",
			fields: fields{
				reader: newTestCountingReader("+ k I 10\n"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad global line",
			fields: fields{
				reader: newTestCountingReader("* i userdata1 testSet1 sindex1 X 1 bin1 N\n"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Decoder[*models.Token]{
				reader:   tt.fields.reader,
				header:   tt.fields.header,
				metaData: tt.fields.metaData,
			}
			got, err := r.NextToken()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.NextToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ASBReader.NextToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewASBReader(t *testing.T) {
	t.Parallel()
	type args struct {
		src io.Reader
	}
	tests := []struct {
		args    args
		want    *Decoder[*models.Token]
		name    string
		wantErr bool
	}{
		{
			name: "positive",
			args: args{
				src: newTestCountingReader(
					"Version 3.1\n" +
						"# namespace ns1\n" +
						"# first-file\na", // "a" appended to avoid EOF error
				),
			},
			want: &Decoder[*models.Token]{
				reader: newTestCountingReader(
					"Version 3.1\n" +
						"# namespace ns1\n" +
						"# first-file\na", // "a" appended to avoid EOF error
				),
				header: &header{
					Version: "3.1",
				},
				metaData: &metaData{
					Namespace: "ns1",
					First:     true,
				},
			},
			wantErr: false,
		},
		{
			name: "negative missing version",
			args: args{
				src: newTestCountingReader(
					"# namespace ns1\n" +
						"# first-file\na", // "a" appended to avoid EOF error
				),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad metadata",
			args: args{
				src: newTestCountingReader(
					"Version 3.1\n" +
						"# badtoken\na", // "a" appended to avoid EOF error
				),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewDecoder[*models.Token](tt.args.src, testFileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewASBReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil && tt.want == nil {
				return
			}
			if !reflect.DeepEqual(got.header, tt.want.header) {
				t.Errorf("NewASBReader() header differs = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got.metaData, tt.want.metaData) {
				t.Errorf("NewASBReader() metadata differs = %v, want %v", got, tt.want)
			}
		})
	}
}
