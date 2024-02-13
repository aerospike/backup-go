package testresources

import (
	"errors"
	"fmt"
	"reflect"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type TestClient struct {
	asc *a.Client
}

type digest = string

type RecordMap map[digest]*a.Record

func NewTestClient(asc *a.Client) *TestClient {
	return &TestClient{
		asc: asc,
	}
}

// TODO allow passing in bins
func (tc *TestClient) WriteRecords(n int, namespace, set string, recs []*a.Record) error {
	for _, rec := range recs {
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

func (tc *TestClient) ValidateRecords(expectedRecs []*a.Record, expCount int, namespace, set string) error {
	actualRecs, err := tc.ReadAllRecords(namespace, set)
	if err != nil {
		return err
	}

	if len(actualRecs) != expCount {
		return errors.New("unexpected number of records")
	}

	for _, expRec := range expectedRecs {
		actual, ok := actualRecs[string(expRec.Key.Digest())]
		if !ok {
			return errors.New("missing record")
		}
		if !reflect.DeepEqual(expRec.Bins, actual.Bins) {
			return fmt.Errorf("wanted bins: %v\n got bins: %v", expRec.Bins, actual.Bins)
		}
	}

	return nil
}

func (tc *TestClient) Truncate(namespace, set string) error {
	return tc.asc.Truncate(nil, namespace, set, nil)
}
