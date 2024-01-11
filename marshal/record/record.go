package record

import (
	"backuplib/marshal/binmap"
	"backuplib/marshal/key"
	"fmt"
	"log"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	citrusLeafEpoch = 1262304000 // pulled from C client cf_clock.h
)

var now = time.Now

// NOTE maybe only define these types for objects that end up in the backup file
// records, sindex, udf, etc
// keys and such should be kept internal and not have a custom type (maybe)
type Record struct {
	a.Record
}

func NewRecord(r a.Record) Record {
	return Record{r}
}

func (r Record) MarshalText() ([]byte, error) {
	var res []byte

	keyText, err := key.NewKey(*r.Key).MarshalText()
	if err != nil {
		return nil, err
	}
	res = append(res, keyText...)

	generationText := fmt.Sprintf("+ g %v\n", r.Generation) // TODO make the data width correct uint16
	res = append(res, generationText...)

	exprTime := getExpirationTime(int64(r.Expiration), now().Unix())
	expirationText := fmt.Sprintf("+ t %v\n", exprTime) // TODO make the data width correct uint16
	res = append(res, expirationText...)

	binCountText := fmt.Sprintf("+ b %v\n", len(r.Bins)) // TODO make the data width correct uint16
	res = append(res, binCountText...)

	binsText, err := binmap.NewBinMap(r.Bins).MarshalText()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	res = append(res, binsText...)

	return res, nil
}

func getExpirationTime(ttl int64, unix_now int64) int64 {
	timeSinceCE := unix_now - citrusLeafEpoch
	return timeSinceCE + ttl
}
