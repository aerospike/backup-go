// Copyright 2024 Aerospike, Inc.
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

package server

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pkg/server/mocks"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Folder names used across tests. All are valid Citrusleaf timestamps:
// > minCitrusleafTS and comfortably below the "now" upper bound for years.
const (
	ts200 = "200000000"
	ts300 = "300000000"
	ts400 = "400000000"
)

// listOutput builds a single-page ListObjectsV2 result from common prefixes.
// The paginator stops after one page because NextContinuationToken is nil.
func listOutput(commonPrefixes ...string) *s3.ListObjectsV2Output {
	cps := make([]types.CommonPrefix, len(commonPrefixes))
	for i, p := range commonPrefixes {
		cps[i] = types.CommonPrefix{Prefix: aws.String(p)}
	}

	return &s3.ListObjectsV2Output{CommonPrefixes: cps}
}

// objectBody wraps a string as a GetObject response body.
func objectBody(s string) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(s))}
}

func metadataJSON(backupID string) string {
	return `{"backup_id":"` + backupID + `","namespace":"ns","format_version":1}`
}

func TestLister_FetchAllMetadata_SortedByBackupID(t *testing.T) {
	m := mocks.NewMockS3API(t)

	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput(ts200+"/", ts300+"/", ts400+"/"), nil)

	// BackupID order is deliberately unrelated to folder order, so the assertion
	// can only pass if the final slice is sorted by BackupID.
	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			switch *in.Key {
			case ts200 + "/metadata.json":
				return objectBody(metadataJSON("b3")), nil
			case ts300 + "/metadata.json":
				return objectBody(metadataJSON("b1")), nil
			case ts400 + "/metadata.json":
				return objectBody(metadataJSON("b2")), nil
			default:
				return nil, &types.NoSuchKey{}
			}
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "b1", got[0].BackupID)
	assert.Equal(t, "b2", got[1].BackupID)
	assert.Equal(t, "b3", got[2].BackupID)
}

func TestLister_FetchAllMetadata_SkipsUnfinishedBackup(t *testing.T) {
	m := mocks.NewMockS3API(t)

	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput(ts200+"/", ts300+"/"), nil)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if *in.Key == ts200+"/metadata.json" {
				return objectBody(metadataJSON("b200")), nil
			}
			// ts300 has no metadata.json -> unfinished backup, must be skipped.
			return nil, &types.NoSuchKey{}
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "b200", got[0].BackupID)
}

func TestLister_FetchAllMetadata_SkipsUnparseableMetadata(t *testing.T) {
	m := mocks.NewMockS3API(t)

	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput(ts200+"/", ts300+"/"), nil)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if *in.Key == ts200+"/metadata.json" {
				return objectBody(metadataJSON("b200")), nil
			}
			// Corrupt JSON -> logged at WARN and skipped, never fails the listing.
			return objectBody("{not valid json"), nil
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "b200", got[0].BackupID)
}

func TestLister_FetchAllMetadata_FiltersNonSnapshotPrefixes(t *testing.T) {
	m := mocks.NewMockS3API(t)

	// "logs/" and "100/" (below minCitrusleafTS) are not snapshots and must be
	// filtered out before any GetObject call.
	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput("logs/", "100/", ts200+"/"), nil)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if !strings.HasPrefix(*in.Key, ts200) {
				t.Errorf("unexpected fetch of non-snapshot prefix: %q", *in.Key)
			}
			return objectBody(metadataJSON("b200")), nil
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "b200", got[0].BackupID)
}

func TestLister_FetchAllMetadata_EmptyListing(t *testing.T) {
	m := mocks.NewMockS3API(t)

	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(&s3.ListObjectsV2Output{}, nil)

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestLister_FetchAllMetadata_ListError(t *testing.T) {
	m := mocks.NewMockS3API(t)

	wantErr := errors.New("s3 unavailable")
	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, wantErr)

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
}

func TestLister_FetchAllMetadata_CancellationReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	m := mocks.NewMockS3API(t)
	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput(ts200+"/"), nil)

	// Cancel mid-fetch: the operation must abort with an error, not return a
	// partial (possibly empty) slice and a nil error.
	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			cancel()
			return nil, context.Canceled
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, got)
}

func TestLister_GetMetadata(t *testing.T) {
	m := mocks.NewMockS3API(t)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if *in.Key == ts200+"/metadata.json" {
				return objectBody(metadataJSON("b200")), nil
			}
			return nil, &types.NoSuchKey{}
		})

	l := NewLister(m, "bucket", "")

	md, err := l.GetMetadata(t.Context(), ts200)
	require.NoError(t, err)
	assert.Equal(t, "b200", md.BackupID)

	_, err = l.GetMetadata(t.Context(), "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMetadataNotFound)
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{s3ErrNoSuchKey, &types.NoSuchKey{}, true},
		{s3ErrNotFound, &types.NotFound{}, true},
		{"wrapped NoSuchKey", errors.Join(errors.New("ctx"), &types.NoSuchKey{}), true},
		{"api 404", &smithy.GenericAPIError{Code: s3Err404}, true},
		{"api NoSuchKey", &smithy.GenericAPIError{Code: s3ErrNoSuchKey}, true},
		{"api NotFound", &smithy.GenericAPIError{Code: s3ErrNotFound}, true},
		{"api AccessDenied", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"plain error", errors.New("boom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isNotFound(tt.err))
		})
	}
}

func TestParseCitrusleafTimestamp(t *testing.T) {
	upper := (time.Now().Unix() - citrusleafEpoch) + clockSkewSeconds

	tests := []struct {
		name string
		in   string
		want int64
		ok   bool
	}{
		{"non-numeric", "abc", 0, false},
		{"below min", "100", 0, false},
		{"exactly min", strconv.FormatInt(minCitrusleafTS, 10), minCitrusleafTS, true},
		{"mid range", ts300, 300000000, true},
		{"exactly upper", strconv.FormatInt(upper, 10), upper, true},
		{"above upper", strconv.FormatInt(upper+1, 10), 0, false},
		{"empty", "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseCitrusleafTimestamp(tt.in)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewLister_Defaults(t *testing.T) {
	m := mocks.NewMockS3API(t)

	l := NewLister(m, "bucket", "some/prefix")
	assert.Equal(t, "some/prefix/", l.prefix, "trailing slash must be appended")
	assert.Equal(t, defaultConcurrency, l.concurrency)
	require.NotNil(t, l.logger, "logger must default to a non-nil discard logger")
}

func TestNewLister_Options(t *testing.T) {
	m := mocks.NewMockS3API(t)

	l := NewLister(m, "bucket", "ready/",
		WithConcurrency(64),
		WithLogger(nil), // nil must be ignored, keeping the default logger
	)

	assert.Equal(t, "ready/", l.prefix, "existing trailing slash must be preserved")
	assert.Equal(t, 64, l.concurrency)
	require.NotNil(t, l.logger)

	// Non-positive concurrency is ignored.
	l2 := NewLister(m, "bucket", "", WithConcurrency(0))
	assert.Equal(t, defaultConcurrency, l2.concurrency)
}
