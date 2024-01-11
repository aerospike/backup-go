package key

import (
	"bytes"
	b64 "encoding/base64"
	"fmt"
	"math"
	"testing"
	"text/template"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type testKey struct {
	namespace string
	setName   string
	val       interface{}
}

var testKeys = []testKey{
	{"test", "", 0},
	{"test", "demo", 0},
	{"test", "", math.MinInt64},
	{"test", "demo", math.MaxInt64},
	{"test", "", "1234"},
	{"test", "demo", "1234"},
	{"tes\"t", "demo", "1234"},
	{"test", "", ""},
	{"test", "", []byte("1234")},
	{"test", "demo", []byte("1234")},
}

// NOTE the newlines matter
const templateString = "{{if (ne .UserKey nil)}}" +
	"{{getTestUserKey .UserKey}}" +
	"{{end}}" +
	"+ n {{.Namespace}}\n" +
	"+ d {{.B64Digest}}\n" +
	"{{if (ne .SetName \"\")}}" +
	"+ s {{.SetName}}\n" +
	"{{end}}"

var templateFuncs = template.FuncMap{
	"getTestUserKey": getTestUserKey,
}
var keyTemplate, _ = template.New("keyTemplate").Funcs(templateFuncs).Parse(templateString)

// TODO see how C backup handles double/float keys
func getTestUserKey(userKey any) string {
	switch userKey.(type) {
	case int:
		return fmt.Sprintf("+ k I %d\n", userKey)
	case string:
		return fmt.Sprintf("+ k S %d %s\n", len(userKey.(string)), userKey)
	case []byte:
		return fmt.Sprintf("+ k B! %d %s\n", len(userKey.([]byte)), userKey) // NOTE old backup file formats have an optional "!" afert "B"
	default:
		panic(fmt.Sprintf("unknown userKey type: %T", userKey))
	}
}

func TestKey_MarshalText(t *testing.T) {
	for _, k := range testKeys {
		var err error

		key, err := a.NewKey(k.namespace, k.setName, k.val)
		if err != nil {
			panic(err)
		}

		var expBuff bytes.Buffer

		type expectedVals struct {
			UserKey   any
			Namespace string
			B64Digest string
			SetName   string
		}

		expVals := expectedVals{
			UserKey:   any(key.Value().GetObject()),
			Namespace: key.Namespace(),
			B64Digest: b64.StdEncoding.EncodeToString(key.Digest()),
			SetName:   key.SetName(),
		}

		err = keyTemplate.Execute(&expBuff, expVals)
		if err != nil {
			panic(err)
		}

		actual, err := NewKey(*key).MarshalText()
		if err != nil {
			t.Error(err)
		}

		if string(actual) != expBuff.String() {
			t.Errorf("\nACTUAL:\n%s\nEXPECTED:\n%s", actual, expBuff.Bytes())
		}
	}
}
