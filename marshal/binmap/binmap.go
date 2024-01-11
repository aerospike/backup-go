package binmap

import (
	"fmt"
	"log"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BinMap struct {
	a.BinMap
}

func NewBinMap(b a.BinMap) BinMap {
	return BinMap{b}
}

func (b BinMap) MarshalText() ([]byte, error) {
	var res []byte

	if len(b.BinMap) < 1 {
		return res, fmt.Errorf("ERR: empty binmap")
	}

	// NOTE golangs random order map iterations
	// means that any backup files that include
	// multi element bin maps will never be the same
	// because the bins are written in random order each time
	for k, v := range b.BinMap {
		binText, err := marshalBin(k, v)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		res = append(res, binText...)
	}

	return res, nil
}

func marshalBin(k string, v any) ([]byte, error) {
	var res []byte

	switch v.(type) {
	case bool:
		return []byte(fmt.Sprintf("- Z %s %t\n", k, v)), nil // TODO verify that the c backup tool writes true/false
	case int: // TODO what about other int/uint?
		return []byte(fmt.Sprintf("- I %s %d\n", k, v)), nil
	case float64:
		return []byte(fmt.Sprintf("- D %s %f\n", k, v)), nil
	case string:
		return []byte(fmt.Sprintf("- S %s %d %s\n", k, len(v.(string)), v)), nil
	case a.HLLValue:
		return []byte(fmt.Sprintf("- Y! %s %d %s\n", k, len(v.([]byte)), v)), nil
	case []byte:
		// TODO I can't find a way to tell what aerospike type of bytes blob is from Go client. e.g. HLL, Python bytes etc.
		return []byte(fmt.Sprintf("- B! %s %d %s\n", k, len(v.([]byte)), v)), nil // TODO add support for language bytes, HLL
	case map[any]any:
		return []byte(fmt.Sprintf("- M! %s %d %s\n", k, len(v.([]byte)), v)), nil // TODO support base64 encoded values with no "!"
	case []any:
		return []byte(fmt.Sprintf("- L! %s %d %s\n", k, len(v.([]byte)), v)), nil
	default:
		return res, fmt.Errorf("unknown user key type: %T, key: %s", v, k)
	}
}
