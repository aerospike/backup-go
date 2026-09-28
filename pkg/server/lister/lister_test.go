// Copyright 2024-2026 Aerospike, Inc.
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

package lister

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aerospike/backup-go/pkg/server/lister/mocks"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Snapshot folder names used across tests. All have the job id form.
const (
	jobID1 = "260316T142035-k3f9"
	jobID2 = "260316T142036-a001"
	jobID3 = "260317T090000-zzzz"
)

const (
	testMetadataFile = "metadata.json"
	testPrefix       = "backups/" + jobID1
	testBackupID     = "b1"
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
		Return(listOutput(jobID1+"/", jobID2+"/", jobID3+"/"), nil)

	// BackupID order is deliberately unrelated to folder order, so the assertion
	// can only pass if the final slice is sorted by BackupID.
	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			switch *in.Key {
			case jobID1 + "/metadata.json":
				return objectBody(metadataJSON("b3")), nil
			case jobID2 + "/metadata.json":
				return objectBody(metadataJSON("b1")), nil
			case jobID3 + "/metadata.json":
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
		Return(listOutput(jobID1+"/", jobID2+"/"), nil)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if *in.Key == jobID1+"/metadata.json" {
				return objectBody(metadataJSON(testBackupID)), nil
			}
			// jobID2 has no metadata.json -> unfinished backup, must be skipped.
			return nil, &types.NoSuchKey{}
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, testBackupID, got[0].BackupID)
}

func TestLister_FetchAllMetadata_SkipsUnparseableMetadata(t *testing.T) {
	m := mocks.NewMockS3API(t)

	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput(jobID1+"/", jobID2+"/"), nil)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if *in.Key == jobID1+"/metadata.json" {
				return objectBody(metadataJSON(testBackupID)), nil
			}
			// Corrupt JSON -> logged at WARN and skipped, never fails the listing.
			return objectBody("{not valid json"), nil
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, testBackupID, got[0].BackupID)
}

func TestLister_FetchAllMetadata_FiltersNonSnapshotPrefixes(t *testing.T) {
	m := mocks.NewMockS3API(t)

	// "logs/" and "100/" do not have the job id form, so they are not snapshots and
	// must be filtered out before any GetObject call.
	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
		Return(listOutput("logs/", "100/", jobID1+"/"), nil)

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if !strings.HasPrefix(*in.Key, jobID1) {
				t.Errorf("unexpected fetch of non-snapshot prefix: %q", *in.Key)
			}
			return objectBody(metadataJSON(testBackupID)), nil
		})

	l := NewLister(m, "bucket", "")

	got, err := l.FetchAllMetadata(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, testBackupID, got[0].BackupID)
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
		Return(listOutput(jobID1+"/"), nil)

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
			if *in.Key == jobID1+"/metadata.json" {
				return objectBody(metadataJSON(testBackupID)), nil
			}
			return nil, &types.NoSuchKey{}
		})

	l := NewLister(m, "bucket", "")

	md, err := l.GetMetadata(t.Context(), jobID1)
	require.NoError(t, err)
	assert.Equal(t, testBackupID, md.BackupID)

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

func TestLister_listSnapshotPrefixes(t *testing.T) {
	t.Parallel()

	const listerPrefix = "backups/"

	tests := []struct {
		name           string
		prefix         string
		commonPrefixes []string
		want           []string
	}{
		{
			name:           "job id folders are kept",
			commonPrefixes: []string{jobID1 + "/", jobID2 + "/", jobID3 + "/"},
			want:           []string{jobID1, jobID2, jobID3},
		},
		{
			name:           "citrusleaf timestamp folders are skipped",
			commonPrefixes: []string{"200000000/", "1758000000/"},
			want:           nil,
		},
		{
			name:           "malformed job ids are skipped",
			commonPrefixes: []string{"260316T142035/", "260316T142035-K3F9/", "260316T142035-k3f/", "logs/"},
			want:           nil,
		},
		{
			name:           "lister prefix is stripped before matching and kept in the result",
			prefix:         listerPrefix,
			commonPrefixes: []string{listerPrefix + jobID1 + "/", listerPrefix + "logs/"},
			want:           []string{listerPrefix + jobID1},
		},
		{
			name:           "empty listing",
			commonPrefixes: nil,
			want:           nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := mocks.NewMockS3API(t)
			m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).
				Return(listOutput(tt.commonPrefixes...), nil)

			l := NewLister(m, "bucket", tt.prefix)

			got, err := l.listSnapshotPrefixes(t.Context())
			require.NoError(t, err)
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

func TestObjectKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		elems []string
		want  string
	}{
		{
			name:  "dot prefix is dropped",
			elems: []string{".", testMetadataFile},
			want:  testMetadataFile,
		},
		{
			name:  "empty prefix is dropped",
			elems: []string{"", testMetadataFile},
			want:  testMetadataFile,
		},
		{
			name:  "root prefix is dropped",
			elems: []string{"/", testMetadataFile},
			want:  testMetadataFile,
		},
		{
			name:  "dot slash prefix is dropped",
			elems: []string{"./", testMetadataFile},
			want:  testMetadataFile,
		},
		{
			name:  "regular prefix is preserved",
			elems: []string{testPrefix, testMetadataFile},
			want:  testPrefix + "/" + testMetadataFile,
		},
		{
			name:  "leading slash is stripped",
			elems: []string{"/" + testPrefix, testMetadataFile},
			want:  testPrefix + "/" + testMetadataFile,
		},
		{
			name:  "duplicated slashes are collapsed",
			elems: []string{testPrefix + "//", testMetadataFile},
			want:  testPrefix + "/" + testMetadataFile,
		},
		{
			name:  "parent traversal is resolved",
			elems: []string{testPrefix + "/sub/..", testMetadataFile},
			want:  testPrefix + "/" + testMetadataFile,
		},
		{
			name:  "no elements",
			elems: nil,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, objectKey(tt.elems...))
		})
	}
}
