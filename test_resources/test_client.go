package testresources

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type DataGenerator interface {
	GenerateRecord(namespace, set string) *a.Record
}

type DataGeneratorFactory interface {
	CreateDataGenerator(*rand.Rand) DataGenerator
}

type TestClient struct {
	asc  *a.Client
	seed *rand.Source
	dgf  DataGeneratorFactory
}

type digest = string

type RecordMap map[digest]*a.Record

func NewTestClient(asc *a.Client, seed *rand.Source, dgf DataGeneratorFactory) *TestClient {
	return &TestClient{
		asc:  asc,
		seed: seed,
		dgf:  dgf,
	}
}

// TODO allow passing in bins
func (tc *TestClient) WriteRecords(n int, namespace, set string) error {
	rng := rand.New(*tc.seed)
	dg := tc.dgf.CreateDataGenerator(rng)

	for i := 0; i < n; i++ {
		rec := dg.GenerateRecord(namespace, set)

		err := tc.asc.Put(nil, rec.Key, rec.Bins)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tc *TestClient) ReadAllRecords(namespace, set string) (RecordMap, error) {
	records := make(RecordMap)

	stmt := a.NewStatement(namespace, set)
	rset, err := tc.asc.Query(nil, stmt)
	if err != nil {
		return nil, err
	}

	rchan := rset.Results()
	for r := range rchan {
		if r.Err != nil {
			return nil, r.Err
		}

		records[string(r.Record.Key.Digest())] = r.Record
	}

	return records, nil
}

func (tc *TestClient) ValidateRecords(records RecordMap, expCount int, namespace, set string) error {
	rng := rand.New(*tc.seed)
	dg := tc.dgf.CreateDataGenerator(rng)

	if len(records) != expCount {
		return errors.New("unexpected number of records")
	}

	for range records {
		expected := dg.GenerateRecord(namespace, set)
		actual, ok := records[string(expected.Key.Digest())]
		if !ok {
			return errors.New("missing record")
		}
		if !reflect.DeepEqual(expected.Bins, actual.Bins) {
			return fmt.Errorf("wanted bins: %v\n got bins: %v", expected.Bins, actual.Bins)
		}
	}

	return nil
}

func (tc *TestClient) Truncate(namespace, set string) error {
	return tc.asc.Truncate(nil, namespace, set, nil)
}
