package record

import (
	"bytes"
	"testing"
	"text/template"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type testBin struct {
	key   string
	value any
}

const expectedKeyText = `+ k S 4 1234
+ n test
+ d 49SdSyAwq68Ip5a+H4pmenMBWrQ=
+ s demo
`

var testKey, _ = a.NewKey("test", "demo", "1234")

const expectedBinText = `- I n 0
`

var testBinMap = a.BinMap{
	"n": 0,
}

type testRecord struct {
	Rec           a.Record
	ExpKeyText    string
	ExpBinText    string
	ExpExpiration int64
}

var testRecords = []testRecord{
	{
		Rec: a.Record{
			Key:        testKey,
			Node:       nil,
			Bins:       testBinMap,
			Generation: 1234,
			Expiration: 9000000,
		},
		ExpKeyText:    expectedKeyText,
		ExpBinText:    expectedBinText,
		ExpExpiration: getTestExpirationTime(9000000, testTime.Unix()),
	},
}

// NOTE the newlines matter
const templateString = "{{.ExpKeyText}}" +
	"+ g {{.Rec.Generation}}\n" +
	"+ t {{.ExpExpiration}}\n" +
	"+ b {{len .Rec.Bins}}\n" +
	"{{.ExpBinText}}"

var keyTemplate, _ = template.New("keyTemplate").Parse(templateString)

func getTestExpirationTime(ttl int64, unix_now int64) int64 {
	timeSinceCE := unix_now - citrusLeafEpoch
	return timeSinceCE + ttl
}

var testTime = time.Now()

func TestRecord_MarshalText(t *testing.T) {
	// override record package time.now function
	now = func() time.Time { return testTime }

	for _, r := range testRecords {
		var err error
		var expBuff bytes.Buffer

		err = keyTemplate.Execute(&expBuff, r)
		if err != nil {
			panic(err)
		}

		actual, err := NewRecord(r.Rec).MarshalText()
		if err != nil {
			t.Error(err)
		}

		if string(actual) != expBuff.String() {
			t.Errorf("\nACTUAL:\n%s\nEXPECTED:\n%s", actual, expBuff.String())
		}
	}
}
