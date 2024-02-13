package testresources

import (
	"math/rand"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type ASDataGenerator struct {
	rng *rand.Rand
}

func NewASDataGenerator(r *rand.Rand) *ASDataGenerator {
	return &ASDataGenerator{
		rng: r,
	}
}

// TODO support other key types (blob, string, etc)
func (dg *ASDataGenerator) GenerateKey(namespace, set string) *a.Key {
	k, err := a.NewKey(namespace, set, dg.rng.Int())
	if err != nil {
		panic(err)
	}

	return k
}

func (dg *ASDataGenerator) GenerateBins() a.BinMap {
	return a.BinMap{
		"bin1": dg.rng.Int(),
	}
}

func (dg *ASDataGenerator) GenerateRecord(namespace, set string) *a.Record {
	return &a.Record{
		Key:  dg.GenerateKey(namespace, set),
		Bins: dg.GenerateBins(),
	}
}
