package key

import (
	b64 "encoding/base64"
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Key struct {
	a.Key
}

func NewKey(k a.Key) Key {
	return Key{k}
}

func (k Key) MarshalText() ([]byte, error) {
	var res []byte

	userKeyText, err := getUserKey(k.Value())
	if err != nil {
		return res, err
	}
	res = append(res, userKeyText...)

	namespaceText := fmt.Sprintf("+ n %s\n", k.Namespace())
	res = append(res, namespaceText...)

	encodedDigest := b64.StdEncoding.EncodeToString(k.Digest())
	digestText := fmt.Sprintf("+ d %s\n", encodedDigest)
	res = append(res, digestText...)

	if k.SetName() != "" {
		setnameText := fmt.Sprintf("+ s %s\n", k.SetName())
		res = append(res, setnameText...)
	}

	return res, nil
}

func getUserKey(userKey a.Value) ([]byte, error) {
	var res []byte

	if userKey == nil {
		return res, nil
	}

	val := userKey.GetObject()

	switch val.(type) {
	case int:
		res = []byte(fmt.Sprintf("+ k I %d\n", val))
	case string:
		res = []byte(fmt.Sprintf("+ k S %d %s\n", len(val.(string)), val))
	case []byte:
		res = []byte(fmt.Sprintf("+ k B! %d %s\n", len(val.([]byte)), val)) // NOTE old backup file formats have an optional "!" afert "B" // TODO does this append a null terminator to the bytes key?
	default:
		return res, fmt.Errorf("ERR: unknown userKey val type: %T", val)
	}

	return res, nil
}
