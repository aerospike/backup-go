package unmarshal

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var asbr = rand.New(rand.NewSource(1))

func randIntn(n int) int {
	return 1 + asbr.Intn(n-1)
}

// randomString generates a string of random characters with length between 0 and n
// if escaped is true, escaped characters may be added to the string
func randomString(n int, escaped bool) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+~`|}{[]:\";'<>?,./")

	// sometimes return an empty string
	if randIntn(10) == 1 {
		return ""
	}

	if escaped {
		letters = append(letters, '\\')
	}

	s := make([]rune, randIntn(n))
	for i := range s {
		// don't let the last char in the string be the escape char
		if i == len(s)-1 && escaped {
			s[i] = letters[randIntn(len(letters)-1)]
			continue
		}

		s[i] = letters[randIntn(len(letters))]
	}
	return string(s)
}

func TestASBReader_readHeader(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		parseErrArgs        parseErrArgs
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
			name: "negative random",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(randomString(40, true)),
				},
			},
			want:    nil,
			wantErr: true,
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
				parseErrArgs:        tt.fields.parseErrArgs,
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

	randNs1 := randomString(15, true)

	type fields struct {
		countingByteScanner countingByteScanner
		parseErrArgs        parseErrArgs
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
					// NOTE a character is appended to the end (in this case "a") to prevent EOF errors
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
			name: "positive random namespace",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(fmt.Sprintf("# namespace %s\n# first-file\na", randNs1)),
				},
			},
			want: &metaData{
				Namespace: randNs1,
				First:     true,
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
			name: "negative random data",
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader("#" + randomString(2000, true)),
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
				parseErrArgs:        tt.fields.parseErrArgs,
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

func unescapeString(s string) string {
	var res string

	var esc bool
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && !esc {
			esc = true
			continue
		}

		esc = false

		res += string(s[i])
	}

	return res
}

func randomSIndex() *SecondaryIndex {
	var res SecondaryIndex

	sindexTypes := []SIndexType{
		BinSIndex,
		ListElementSIndex,
		MapKeySIndex,
		MapValueSIndex,
	}

	namespace := randomString(100, true)
	set := randomString(50, true)
	name := randomString(100, true)
	indexType := sindexTypes[randIntn(len(sindexTypes))]

	paths := make([]*SIndexPath, randIntn(4))
	for i := range paths {
		paths[i] = randomSIndexPath()
	}

	valuesCovered := len(paths)

	res.Namespace = namespace
	res.Set = set
	res.Name = name
	res.IndexType = indexType
	res.Paths = paths
	res.ValuesCovered = valuesCovered

	return &res
}

func randomSIndexPath() *SIndexPath {
	var res SIndexPath

	sindexPathTypes := []SIPathBinType{
		NumericSIDataType,
		StringSIDataType,
		GEO2DSphereSIDataType,
		BlobSIDataType,
	}

	res.BinName = randomString(50, true)
	res.BinType = sindexPathTypes[randIntn(len(sindexPathTypes))]

	return &res
}

func sindexToString(sindex *SecondaryIndex) string {
	var res string

	res = fmt.Sprintf(" %s %s %s %c %d", sindex.Namespace, sindex.Set, sindex.Name, sindex.IndexType, sindex.ValuesCovered)

	for _, path := range sindex.Paths {
		res += fmt.Sprintf(" %s", sindexPathToString(path))
	}

	return res
}

func sindexPathToString(path *SIndexPath) string {
	return fmt.Sprintf("%s %c", path.BinName, path.BinType)
}

func getUnescapedSIndex(sindex *SecondaryIndex) *SecondaryIndex {
	var res SecondaryIndex

	res.Namespace = unescapeString(sindex.Namespace)
	res.Set = unescapeString(sindex.Set)
	res.Name = unescapeString(sindex.Name)
	res.IndexType = sindex.IndexType
	res.ValuesCovered = sindex.ValuesCovered

	res.Paths = make([]*SIndexPath, len(sindex.Paths))
	for i := range sindex.Paths {
		res.Paths[i] = getUnescapedSIndexPath(sindex.Paths[i])
	}

	return &res
}

func getUnescapedSIndexPath(path *SIndexPath) *SIndexPath {
	var res SIndexPath

	res.BinName = unescapeString(path.BinName)
	res.BinType = path.BinType

	return &res
}

func TestASBReader_readSIndex(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		parseErrArgs        parseErrArgs
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    *SecondaryIndex
		wantErr bool
	}

	randPositiveTests := make([]test, 1000)
	for i := range randPositiveTests {
		randSI := randomSIndex()
		randSIText := sindexToString(randSI)

		randPositiveTests[i] = test{
			name: "positive random " + fmt.Sprint(i),
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(randSIText + "\n"),
				},
			},
			want:    getUnescapedSIndex(randSI),
			wantErr: false,
		}
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

	tests = append(tests, randPositiveTests...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "positive random 48" {
				fmt.Println("test")
			}
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				parseErrArgs:        tt.fields.parseErrArgs,
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
