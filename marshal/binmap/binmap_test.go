package binmap

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"text/template"
)

type testBin struct {
	key   string
	value any
}

// TODO HLL test
var testBinMaps = []map[string]any{
	{
		"n":              0,
		"testmaxxlength": 0,
		"testMinInt":     math.MinInt,
		"testMaxInt":     math.MaxInt,
		"testMaxFloat":   math.MaxFloat64,
		"testFloat":      0.0,
		"testString":     "test_string",
		"testKey":        []byte("test_bytes"),
		"testTrue":       true,
		"testFalse":      false,
	},
}

// NOTE the newlines matter
const templateString = "{{range $key, $value := .}}" +
	"{{getTestBin $key $value}}" +
	"{{end}}"

var templateFuncs = template.FuncMap{
	"getTestBin": getTestUserBin,
}
var keyTemplate, _ = template.New("keyTemplate").Funcs(templateFuncs).Parse(templateString)

type genericMap interface {
	~map[any]any
}

func getTestUserBin(k string, v any) string {
	switch v.(type) {
	case bool:
		return fmt.Sprintf("- Zh %s %t\n", k, v) // TODO verify that the c backup tool writes true/false
	case int: // TODO what about other int/uint?
		return fmt.Sprintf("- I %s %d\n", k, v)
	case float64:
		return fmt.Sprintf("- D %s %f\n", k, v)
	case string:
		return fmt.Sprintf("- S %s %d %s\n", k, len(v.(string)), v)
	case []byte:
		return fmt.Sprintf("- B! %s %d %s\n", k, len(v.([]byte)), v) // TODO add support for language bytes, maps, lists, HLL
	default:
		panic(fmt.Sprintf("unknown userKey type: %T, key: %s", v, k))
	}
}

func sortOutput(s string) string {
	var sorted sort.StringSlice
	sorted = strings.Split(s, "\n")
	sorted.Sort()
	return strings.Join(sorted, "\n")
}

func TestBinMap_MarshalText(t *testing.T) {
	for _, b := range testBinMaps {
		var err error
		var expBuff bytes.Buffer

		err = keyTemplate.Execute(&expBuff, b)
		if err != nil {
			panic(err)
		}

		actual, err := NewBinMap(b).MarshalText()
		if err != nil {
			t.Error(err)
		}

		sortedActual := sortOutput(string(actual))
		sortedExpected := sortOutput(expBuff.String())

		if sortedActual != sortedExpected {
			t.Errorf("\nACTUAL:\n%s\nEXPECTED:\n%s", actual, expBuff.Bytes())
		}
	}
}
