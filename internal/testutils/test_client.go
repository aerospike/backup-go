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
	"errors"
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
)

// TestClient is a wrapper around the Aerospike client that provides
// convenience methods for testing.
type TestClient struct {
	asc *a.Client
	inf *asinfo.InfoClient
}

type digest = string

// RecordMap is a map of record digests to records.
// It is used to compare expected and actual records.
// The digest is a string representation of the record's key digest.
type RecordMap map[digest]*a.Record

// NewTestClient creates a new TestClient.
func NewTestClient(asc *a.Client) *TestClient {
	infoPolicy := a.NewInfoPolicy()

	infoClient, err := asinfo.NewInfoClientFromAerospike(asc, infoPolicy)
	if err != nil {
		panic(err)
	}

	return &TestClient{
		asc: asc,
		inf: infoClient,
	}
}

// WriteUDFs writes a UDF to the database.
func (tc *TestClient) WriteUDFs(udfs []*models.UDF) error {
	for _, udf := range udfs {
		var UDFLang a.Language

		switch udf.UDFType {
		case models.UDFTypeLUA:
			UDFLang = a.LUA
		default:
			return errors.New("error registering UDF: invalid UDF language")
		}

		job, err := tc.asc.RegisterUDF(nil, udf.Content, udf.Name, UDFLang)
		if err != nil {
			return err
		}

		errs := job.OnComplete()

		err = <-errs
		if err != nil {
			return err
		}
	}

	return nil
}

// DropUDF deletes a UDF from the database.
func (tc *TestClient) DropUDF(name string) error {
	job, err := tc.asc.RemoveUDF(nil, name)
	if err != nil {
		return err
	}

	errs := job.OnComplete()

	return <-errs
}

// ReadAllUDFs reads all UDFs in the database.
func (tc *TestClient) ReadAllUDFs() ([]*models.UDF, error) {
	return tc.inf.GetUDFs()
}

// WriteSIndexes writes a secondary index to the database.
func (tc *TestClient) WriteSIndexes(sindexes []*models.SIndex) error {
	for _, sindex := range sindexes {
		sindexType, err := getIndexType(sindex)
		if err != nil {
			return err
		}

		sindexCollectionType, err := getSindexCollectionType(sindex)
		if err != nil {
			return err
		}

		var ctx []*a.CDTContext

		if sindex.Path.B64Context != "" {
			var err error

			ctx, err = a.Base64ToCDTContext(sindex.Path.B64Context)
			if err != nil {
				return err
			}
		}

		task, err := tc.asc.CreateComplexIndex( // TODO create complex indexes
			nil,
			sindex.Namespace,
			sindex.Set,
			sindex.Name,
			sindex.Path.BinName,
			sindexType,
			sindexCollectionType,
			ctx...,
		)
		if err != nil {
			return err
		}

		errs := task.OnComplete()

		err = <-errs
		if err != nil {
			return err
		}
	}

	return nil
}

func getSindexCollectionType(sindex *models.SIndex) (a.IndexCollectionType, error) {
	switch sindex.IndexType {
	case models.BinSIndex:
		return a.ICT_DEFAULT, nil
	case models.ListElementSIndex:
		return a.ICT_LIST, nil
	case models.MapKeySIndex:
		return a.ICT_MAPKEYS, nil
	case models.MapValueSIndex:
		return a.ICT_MAPVALUES, nil
	}
	return 0, fmt.Errorf("invalid sindex collection type: %c", sindex.IndexType)
}

func getIndexType(sindex *models.SIndex) (a.IndexType, error) {
	switch sindex.Path.BinType {
	case models.NumericSIDataType:
		return a.NUMERIC, nil
	case models.StringSIDataType:
		return a.STRING, nil
	case models.BlobSIDataType:
		return a.BLOB, nil
	case models.GEO2DSphereSIDataType:
		return a.GEO2DSPHERE, nil
	}
	return "", fmt.Errorf("invalid sindex bin type: %c", sindex.Path.BinType)
}

// DropSIndex deletes a secondary index from the database.
func (tc *TestClient) DropSIndex(namespace, set, name string) error {
	err := tc.asc.DropIndex(nil, namespace, set, name)
	return err
}

// ReadAllSIndexes reads all secondary indexes in the given namespace.
func (tc *TestClient) ReadAllSIndexes(namespace string) ([]*models.SIndex, error) {
	sindexes, err := tc.inf.GetSIndexes(namespace)
	if err != nil {
		return nil, err
	}

	return sindexes, nil
}

// ValidateSIndexes compares the expected secondary indexes to the actual secondary indexes in the database.
func (tc *TestClient) ValidateSIndexes(t assert.TestingT, expected []*models.SIndex, namespace string) {
	actual, err := tc.ReadAllSIndexes(namespace)
	if err != nil {
		t.Errorf("Error reading sindexes: %v", err)
	}

	assert.Equal(t, expected, actual)
}

// WriteRecords writes the given records to the database.
func (tc *TestClient) WriteRecords(recs []*a.Record) error {
	for _, rec := range recs {
		err := tc.asc.Put(nil, rec.Key, rec.Bins)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadAllRecords reads all records from the given namespace and set.
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

// ValidateRecords compares the expected records to the actual records in the database.
// It fails if the number of records in the namespace and set does not match the length of
// the expected records, or if any unexpected records are found in the database.
// It does this by reading all records in the database namespace and set, then comparing
// their digests and bins to the expected records' digests and bins.
// Currently, it does not compare the records' metadata, only their digests and bins.
// TODO compare metadata and user keys, maybe in another method
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

// Truncate deletes all records in the given namespace and set.
func (tc *TestClient) Truncate(namespace, set string) error {
	return tc.asc.Truncate(nil, namespace, set, nil)
}
