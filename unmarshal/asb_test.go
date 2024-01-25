package unmarshal

import (
	"encoding/base64"
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

// TODO further randomize the letter set
// randomString generates a string of random characters with length between 0 and n
// if escaped is true, escaped characters may be added to the string
func randomString(n int, escaped bool, allowControlChars bool) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+~`|}{[]:\";'<>?,./")
	var controlChars = []rune("\n\\ ")

	slen := asbr.Intn(n)
	s := []rune{}
	for len(s) < slen {
		chars := ""
		usedControlChar := false

		if allowControlChars {
			if asbr.Intn(len(letters)-len(controlChars)) == 1 {
				if escaped {
					chars += "\\"
				}

				chars += string(controlChars[asbr.Intn(len(controlChars))])
				usedControlChar = true
			}
		}

		if !usedControlChar {
			chars += string(letters[asbr.Intn(len(letters))])
		}

		s = append(s, []rune(chars)...)
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
					ByteScanner: strings.NewReader(randomString(40, false, true)),
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

	randNs1 := randomString(15, true, true)

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
					ByteScanner: strings.NewReader("#" + randomString(2000, false, true)),
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

	namespace := randomString(100, true, true)
	set := randomString(50, true, true)
	name := randomString(100, true, true)
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

	res.BinName = randomString(50, true, true)
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

type asbTestBin struct {
	BinType              byte
	Value                any
	Name                 string
	StringRepresentation string
	encoded              bool
}

func randomAsbTestBin() *asbTestBin {
	var res asbTestBin

	var binTypeList []byte
	for t := range binTypes {
		binTypeList = append(binTypeList, t)
	}

	bt := binTypeList[randIntn(len(binTypeList))]
	res.BinType = bt
	res.StringRepresentation += string(bt)

	// this applies to bytes bins that are not base64 encoded
	compressed := false
	if _, ok := bytesBinTypes[bt]; ok {
		if randIntn(2) == 1 {
			compressed = true
			res.StringRepresentation += "!"
		}
	}

	res.StringRepresentation += " "

	binName := randomString(100, true, true)
	res.StringRepresentation += binName
	res.Name = binName

	if bt == 'U' {
		// ignore LDT case
		return &res
	}

	if bt == 'N' {
		res.Value = nil
		res.StringRepresentation += "\n"
		return &res
	}

	res.StringRepresentation += " "

	var binVal any
	var strVal string

	switch bt {
	case 'Z':
		// bool is rendered as T or F so it is a special case
		val := randIntn(2) == 1
		if val {
			strVal = "T"
		} else {
			strVal = "F"
		}
		binVal = val
	case 'I':
		binVal = asbr.Int63()
		strVal = fmt.Sprint(binVal)
	case 'D':
		binVal = asbr.Float64()
		strVal = fmt.Sprint(binVal)
	case 'S':
		val := randomString(100, false, true)
		res.StringRepresentation += fmt.Sprint(len(val))
		res.StringRepresentation += " "
		binVal = val
		strVal = val
	case 'X':
		randStr := randomString(100, false, true)
		val := base64.StdEncoding.EncodeToString([]byte(randStr))
		res.StringRepresentation += fmt.Sprint(len(val))
		res.StringRepresentation += " "
		binVal = randStr
		strVal = val
		res.encoded = true
	case 'G':
		val := randomString(100, false, true)
		res.StringRepresentation += fmt.Sprint(len(val))
		res.StringRepresentation += " "
		strVal = val
		binVal = val
	}

	if _, ok := bytesBinTypes[bt]; ok {
		var val []byte
		if !compressed {
			randStr := randomString(100, false, true)
			val = []byte(base64.StdEncoding.EncodeToString([]byte(randStr)))
			strVal = string(val)
			res.encoded = true
		} else {
			randStr := randomString(100, false, true)
			strVal = randStr
			val = []byte(randStr)
		}
		res.StringRepresentation += fmt.Sprint(len(val))
		res.StringRepresentation += " "
		binVal = val
	}

	res.Value = binVal
	res.StringRepresentation += strVal
	res.StringRepresentation += "\n"

	return &res
}

type asbTestBins struct {
	Bins                 map[string]any
	StringRepresentation string
}

func randomAsbTestBins() *asbTestBins {
	var res asbTestBins

	res.Bins = make(map[string]any)
	for i := 0; i < randIntn(15); i++ {
		bin := randomAsbTestBin()
		res.Bins[bin.Name] = bin.Value

		res.StringRepresentation += fmt.Sprintf("%c %s", recordBinsMarker, bin.StringRepresentation)
	}

	return &res
}

func TestASBReader_readBin(t *testing.T) {
	type fields struct {
		countingByteScanner countingByteScanner
		parseErrArgs        parseErrArgs
		header              *header
		metaData            *metaData
	}

	type test struct {
		name    string
		fields  fields
		want    map[string]any
		wantErr bool
	}

	randPositiveTests := make([]test, 100)

	for i := range randPositiveTests {
		randBin := randomAsbTestBin()
		randBinText := randBin.StringRepresentation
		randPositiveTests[i] = test{
			name: "positive random " + fmt.Sprint(i),
			fields: fields{
				countingByteScanner: countingByteScanner{
					ByteScanner: strings.NewReader(randBinText),
				},
			},
			want:    map[string]any{unescapeString(randBin.Name): randBin.Value},
			wantErr: false,
		}
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
	}

	tests = append(tests, randPositiveTests...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "positive random 10" {
				fmt.Println("test")
			}
			r := &ASBReader{
				countingByteScanner: tt.fields.countingByteScanner,
				parseErrArgs:        tt.fields.parseErrArgs,
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

// func randomRecord() *Record {
// 	var res Record

// 	res.Generation = uint32(asbr.Int31())
// 	res.Expiration = uint32(asbr.Int31())
// 	res.Key = randomKey()

// 	res.Bins = make(map[string]any)
// 	for i := 0; i < randIntn(10); i++ {
// 		res.Bins[randomString(100, true)] = randomBin()
// 	}

// 	return &res
// }

// func randomKey() *a.Key {
// 	var res *a.Key

// 	namespace := randomString(100, true)
// 	set := randomString(100, true)
// 	userKey := randomString(100, false)

// 	digest := make([]byte, 20)
// 	// NOTE asbr.Read() is not thread safe
// 	asbr.Read(digest)

// 	res, err := a.NewKeyWithDigest(namespace, set, userKey, digest)
// 	if err != nil {
// 		// for testing use only
// 		panic(err)
// 	}

// 	return res
// }

// func TestASBReader_readRecord(t *testing.T) {
// 	type fields struct {
// 		countingByteScanner countingByteScanner
// 		parseErrArgs        parseErrArgs
// 		header              *header
// 		metaData            *metaData
// 	}

// 	type test struct {
// 		name    string
// 		fields  fields
// 		want    *Record
// 		wantErr bool
// 	}

// 	randPositiveTests := make([]test, 1000)
// 	for i := range randPositiveTests {
// 		randRec := randomRecord()
// 		randRecText := recordToString(randRec)

// 		randPositiveTests[i] = test{
// 			name: "positive random " + fmt.Sprint(i),
// 			fields: fields{
// 				countingByteScanner: countingByteScanner{
// 					ByteScanner: strings.NewReader(randRecText + "\n"),
// 				},
// 			},
// 			want:    getUnescapedRecord(randRec),
// 			wantErr: false,
// 		}
// 	}

// 	tests := []test{
// 		{
// 			name: "positive empty record",
// 			fields: fields{
// 				countingByteScanner: countingByteScanner{
// 					ByteScanner: strings.NewReader(""),
// 				},
// 			},
// 			want: &Record{
// 				Generation: 0,
// 				Expiration: 0,
// 				TTL:        0,
// 				Key:        "",
// 				Bins:       nil,
// 			},
// 			wantErr: false,
// 		},
// 		{
// 			name: "positive simple",
// 			fields: fields{
// 				countingByteScanner: countingByteScanner{
// 					ByteScanner: strings.NewReader("1 2 3 key1 1 bin1 1 1 1 1 1 1 1 1 1 1 1 1 1 1\n"),
// 				},
// 			},
// 			want: &Record{
// 				Generation: 1,
// 				Expiration: 2,
// 				TTL:        3,
// 				Key:        "key1",
// 				Bins: map[string]interface{}{
// 					"bin1": 1,
// 				},
// 			},
// 			wantErr: false,
// 		},
// 		// TODO fill in negative tests similar to readSIndex
// 	}
// 	tests = append(tests, randPositiveTests...)

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if tt.name == "positive random 48" {
// 				fmt.Println("test")
// 			}
// 			r := &ASBReader{
// 				countingByteScanner: tt.fields.countingByteScanner,
// 				parseErrArgs:        tt.fields.parseErrArgs,
// 				header:              tt.fields.header,
// 				metaData:            tt.fields.metaData,
// 			}
// 			got, err := r.readRecord()
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ASBReader.readRecord() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if diff := cmp.Diff(got, tt.want); diff != "" {
// 				t.Errorf("ASBReader.readSIndex() mismatch:\n%s", diff)
// 			}
// 		})
// 	}
// }
