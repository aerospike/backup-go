// Copyright 2024-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutils

import (
	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/assert"
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

func (tc *TestClient) WriteRecords(recs []*a.Record) error {
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

func (tc *TestClient) ValidateRecords(
	t assert.TestingT, expectedRecs []*a.Record, expCount int, namespace, set string) {
	actualRecs, err := tc.ReadAllRecords(namespace, set)
	if err != nil {
		t.Errorf("Error reading records: %v", err)
	}

	if len(actualRecs) != expCount {
		t.Errorf("Expected %d records, got %d", expCount, len(actualRecs))
	}

	for _, expRec := range expectedRecs {
		actual, ok := actualRecs[string(expRec.Key.Digest())]
		if !ok {
			t.Errorf("Expected record not found: %v", expRec.Key)
		}

		assert.Equal(t, expRec.Bins, actual.Bins)
	}
}

func (tc *TestClient) Truncate(namespace, set string) error {
	return tc.asc.Truncate(nil, namespace, set, nil)
}
