package unmarshal

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/google/go-cmp/cmp"
)

func TestASBReader_readHeader(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}
	tests := []struct {
		name    string
		fields  fields
		want    *header
		wantErr bool
	}{
		{
			name: "positive simple",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("Version 3.1\n"),
				},
			},
			want: &header{
				Version: "3.1",
			},
			wantErr: false,
		},
		{
			name: "negative missing line feed",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("Version 3.1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("Version3.1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad version number",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("Version 31"),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got, err := r.readHeader()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("ASBReader.readHeader() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readMetadata(t *testing.T) {

	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}
	tests := []struct {
		name    string
		fields  fields
		want    *metaData
		wantErr bool
	}{
		{
			name: "positive escaped namespace",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# namespace ns\\\n1\n# first-file\na"),
				},
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
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# namespace customers9\nd"),
				},
			},
			want: &metaData{
				Namespace: "customers9",
			},
			wantErr: false,
		},
		{
			name: "positive no namespace",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# first-file\na"),
				},
			},
			want: &metaData{
				First: true,
			},
			wantErr: false,
		},
		{
			name: "negative empty metadata line",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# "),
				},
			},
			want:    nil,
			wantErr: true, // this will be the EOF error
		},
		{
			name: "negative missing space",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("#namespace hugerecords\n# first-file\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing second space",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# namespace dergin3\n#first-file\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad token",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# namespace tester\n# bad-token\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad namespace format (no new line)",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# namespace ns1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad first-file (no new line)",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("# first-file bad-data"),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got, err := r.readMetadata()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("ASBReader.readMetadata() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readSIndex(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    *SecondaryIndex
		wantErr bool
	}

	tests := []test{
		{
			name: "positive bin numeric",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 N 1 bin1 N\n"),
				},
			},
			want: &SecondaryIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: BinSIndex,
				Paths: []*SIndexPath{
					{
						BinName: "bin1",
						BinType: NumericSIDataType,
					},
				},
				ValuesCovered: 1,
			},
			wantErr: false,
		},
		{
			name: "positive ListElement string",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 L 1 bin1 S\n"),
				},
			},
			want: &SecondaryIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: ListElementSIndex,
				Paths: []*SIndexPath{
					{
						BinName: "bin1",
						BinType: StringSIDataType,
					},
				},
				ValuesCovered: 1,
			},
			wantErr: false,
		},
		{
			name: "positive mapKey geo2dsphere",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 K 1 bin1 G\n"),
				},
			},
			want: &SecondaryIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: MapKeySIndex,
				Paths: []*SIndexPath{
					{
						BinName: "bin1",
						BinType: GEO2DSphereSIDataType,
					},
				},
				ValuesCovered: 1,
			},
			wantErr: false,
		},
		{
			name: "positive mapValue blob",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 V 1 bin1 B\n"),
				},
			},
			want: &SecondaryIndex{
				Namespace: "userdata1",
				Set:       "testSet1",
				Name:      "sindex1",
				IndexType: MapValueSIndex,
				Paths: []*SIndexPath{
					{
						BinName: "bin1",
						BinType: BlobSIDataType,
					},
				},
				ValuesCovered: 1,
			},
			wantErr: false,
		},
		{
			name: "negative missing first space",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("userdata1 testSet1 sindex1 V 1 bin1 B\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after namespace",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after set",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after name",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid index type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 Z 1 bin1 B\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after index type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 V\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing size",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 V  bin1 B\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after size",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("userdata1 testSet1 sindex1 V 1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after path name",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 V 1 bin1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid path type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 V 1 bin1 Z\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing new line",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" userdata1 testSet1 sindex1 V 1 bin1 B"),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "positive random 48" {
				fmt.Println("test")
			}
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got, err := r.readSIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readSIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("ASBReader.readSIndex() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readUDF(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    *UDF
		wantErr bool
	}

	tests := []test{
		{
			name: "positive lua udf",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" L lua-udf 11 lua-content\n"),
				},
			},
			want: &UDF{
				UDFType: LUAUDFType,
				Name:    "lua-udf",
				Content: []byte("lua-content"),
			},
			wantErr: false,
		},
		{
			name: "negative missing space after udf type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" Llua-udf 11 lua-content\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after udf name",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" L lua-udf11 lua-content\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing length",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" L lua-udf  lua-content\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after length",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" L lua-udf 11lua-content\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing content",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" L lua-udf 11 \n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing new line",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(" L lua-udf 11 lua-content"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing starting space",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("L lua-udf 11 lua-content\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got, err := r.readUDF()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readUDF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("ASBReader.readUDF() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readBin(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    map[string]any
		wantErr bool
	}

	tests := []test{
		{
			name: "positive nil bin",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("N nil-bin\n"),
				},
			},
			want: map[string]any{
				"nil-bin": nil,
			},
			wantErr: false,
		},
		{
			name: "positive int bin",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("I int-bin 1\n"),
				},
			},
			want: map[string]any{
				"int-bin": int64(1),
			},
			wantErr: false,
		},
		{
			name: "positive string bin",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("S string-bin 6 string\n"),
				},
			},
			want: map[string]any{
				"string-bin": "string",
			},
			wantErr: false,
		},
		{
			name: "positive blob bin",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("B blob-bin %d %s\n", base64.StdEncoding.EncodedLen(3), base64.StdEncoding.EncodeToString([]byte{0, 1, 2}))),
				},
			},
			want: map[string]any{
				"blob-bin": []byte{0, 1, 2},
			},
			wantErr: false,
		},
		{
			name: "positive list bin",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("L list-bin %d %s\n", base64.StdEncoding.EncodedLen(3), base64.StdEncoding.EncodeToString([]byte{1, 2, 3}))),
				},
			},
			want: map[string]any{
				"list-bin": []byte{1, 2, 3},
			},
			wantErr: false,
		},
		{
			name: "negative missing space after bin type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("Nnil-bin\n"),
				},
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative bad end char after bin name",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("N nil-bin|"),
				},
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative missing space after bin name",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("I int-bin\n"),
				},
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative missing line feed after int bin value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("I int-bin 1"),
				},
			},
			want:    map[string]any{},
			wantErr: true,
		},
		{
			name: "negative LDT bin",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("U ldt-bin 1\n"),
				},
			},
			want:    map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "positive random 10" {
				fmt.Println("test")
			}
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got := make(map[string]any)
			err := r.readBin(got)
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readBins() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("ASBReader.readBins() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readKey(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    any
		wantErr bool
	}

	tests := []test{
		{
			name: "positive int key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("I 1\n"),
				},
			},
			want:    int64(1),
			wantErr: false,
		},
		{
			name: "positive float key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("D 1.1\n"),
				},
			},
			want:    float64(1.1),
			wantErr: false,
		},
		{
			name: "positive string key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("S 6 string\n"),
				},
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "positive base64 string key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("X %d %s\n", base64.StdEncoding.EncodedLen(6), base64.StdEncoding.EncodeToString([]byte("string")))),
				},
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "positive blob key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("B %d %s\n", base64.StdEncoding.EncodedLen(3), base64.StdEncoding.EncodeToString([]byte{0, 1, 2}))),
				},
			},
			want:    []byte{0, 1, 2},
			wantErr: false,
		},
		{
			name: "positive compressed blob key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("B! 3 123\n"),
				},
			},
			want:    []byte("123"),
			wantErr: false,
		},
		{
			name: "negative bad key type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("Z 1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after key type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("I1\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after !",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("B!3 123\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after int key value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("I 1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after float key value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("D 1.1"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after string key value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("S 6 string"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after base64 string key value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("X %d %s", base64.StdEncoding.EncodedLen(6), base64.StdEncoding.EncodeToString([]byte("string")))),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after blob key value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("B %d %s", base64.StdEncoding.EncodedLen(3), base64.StdEncoding.EncodeToString([]byte{0, 1, 2}))),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing line feed after compressed blob key value",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("B! 3 123"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after string size",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("S 6string\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after base64 string size",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("X %d%s\n", base64.StdEncoding.EncodedLen(6), base64.StdEncoding.EncodeToString([]byte("string")))),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after blob size",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("B %d%s\n", base64.StdEncoding.EncodedLen(3), base64.StdEncoding.EncodeToString([]byte{0, 1, 2}))),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got, err := r.readUserKey()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("ASBReader.readKey() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readRecord(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    *Record
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
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want: &Record{
				Key: intKey,
				Bins: map[string]any{
					"bin1": nil,
					"bin2": int64(2),
				},
				Generation: 10,
				Expiration: 10,
			},
			wantErr: false,
		},
		{
			name: "positive set and no key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want: &Record{
				Key: keyNoUserVal,
				Bins: map[string]any{
					"bin1": nil,
					"bin2": int64(2),
				},
				Generation: 10,
				Expiration: 10,
			},
			wantErr: false,
		},
		{
			name: "positive no set and no key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
						"+ n namespace1\n" +
							"+ d " + encodedDigest + "\n" +
							"+ g 10\n" +
							"+ t 10\n" +
							"+ b 2\n" +
							"- N bin1\n" +
							"- I bin2 2\n",
					),
				},
			},
			want: &Record{
				Key: keyNoUserValOrSet,
				Bins: map[string]any{
					"bin1": nil,
					"bin2": int64(2),
				},
				Generation: 10,
				Expiration: 10,
			},
			wantErr: false,
		},
		{
			name: "negative bad starting char",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
						"x n namespace1\n" +
							"+ d " + encodedDigest + "\n" +
							"+ g 10\n" +
							"+ t 10\n" +
							"+ b 2\n" +
							"- N bin1\n" +
							"- I bin2 2\n",
					),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing starting char",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
						"n namespace1\n" +
							"+ d " + encodedDigest + "\n" +
							"+ g 10\n" +
							"+ t 10\n" +
							"+ b 2\n" +
							"- N bin1\n" +
							"- I bin2 2\n",
					),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after starting char",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
						"+n namespace1\n" +
							"+ d " + encodedDigest + "\n" +
							"+ g 10\n" +
							"+ t 10\n" +
							"+ b 2\n" +
							"- N bin1\n" +
							"- I bin2 2\n",
					),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative invalid record line type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
						"+ Z namespace1\n" +
							"+ d " + encodedDigest + "\n" +
							"+ g 10\n" +
							"+ t 10\n" +
							"+ b 2\n" +
							"- N bin1\n" +
							"- I bin2 2\n",
					),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing space after record line type",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing namespace",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing digest",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing generation",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing expiration",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing bin count",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative missing bins",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
						"+ k I 10\n" +
							"+ n namespace1\n" +
							"+ d " + encodedDigest + "\n" +
							"+ s set1\n" +
							"+ g 10\n" +
							"+ t 10\n" +
							"+ b 2\n",
					),
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad user key",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad namespace",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad digest",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad set",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad generation",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad expiration",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "negative bad bin count",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(
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
			},
			want:    nil,
			wantErr: true,
		},
	}

	// allow comp.Diff to compare aerospike keys
	keyCmp := cmp.Comparer(func(x, y *a.Key) bool {
		if x == nil || y == nil {
			return x == y
		}
		if !x.Equals(y) {
			return false
		}
		return x.String() == y.String()
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
			}
			got, err := r.readRecord()
			if (err != nil) != tt.wantErr {
				t.Errorf("ASBReader.readRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, keyCmp); diff != "" {
				t.Errorf("ASBReader.readRecord() mismatch:\n%s", diff)
			}
		})
	}
}

func TestASBReader_readBinCount(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		header              *header
		metaData            *metaData
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint16
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				header:              tt.fields.header,
				metaData:            tt.fields.metaData,
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
