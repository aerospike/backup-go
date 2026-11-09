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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aerospike/backup-go/pkg/server/lister/mocks"
	"github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	statsKey  = "backups/" + jobID1
	statsNode = "BB9D5F76B77F4A6"
	// statsPageSize keeps listing pages small, so a listing spans several pages.
	statsPageSize = 2
)

// queryManifestJSON is a query stream manifest of a partition with base and
// incr counts and two segments.
func queryManifestJSON(baseRecords, incrRecords, baseBytes, incrBytes int) string {
	return `{"backup_id":"` + jobID1 + `","partition_id":1053,"partition_complete":true,` +
		`"num_base_records":` + strconv.Itoa(baseRecords) + `,"num_incr_records":` + strconv.Itoa(incrRecords) +
		`,"num_base_bytes":` + strconv.Itoa(baseBytes) + `,"num_incr_bytes":` + strconv.Itoa(incrBytes) +
		`,"segments":[{"segment_name":"a.seg","size":1,"record_count":1},{"segment_name":"b.seg","size":1}]}`
}

// changeManifestJSON is a change stream manifest of a node with one segment.
func changeManifestJSON(records, bytes int) string {
	return `{"backup_id":"` + jobID1 + `","lane":"change","num_records":` + strconv.Itoa(records) +
		`,"num_bytes":` + strconv.Itoa(bytes) + `,"segments":[{"segment_name":"c.seg","size":1}]}`
}

// mockBucket serves objects to m as S3 would: listings with a delimiter, split
// into pages of statsPageSize entries, and reads by key.
func mockBucket(t *testing.T, objects map[string]string) *mocks.MockS3API {
	t.Helper()

	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	m := mocks.NewMockS3API(t)

	m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return listPage(keys, aws.ToString(in.Prefix), aws.ToString(in.Delimiter),
				aws.ToString(in.ContinuationToken)), nil
		}).Maybe()

	m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			body, ok := objects[aws.ToString(in.Key)]
			if !ok {
				return nil, &types.NoSuchKey{}
			}

			return objectBody(body), nil
		}).Maybe()

	return m
}

// listPage lists the keys under prefix, rolled up at delimiter, from the entry
// after token, which is the index of the last entry of the previous page.
func listPage(keys []string, prefix, delimiter, token string) *s3.ListObjectsV2Output {
	type entry struct {
		key   string
		isDir bool
	}

	var entries []entry

	for _, key := range keys {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}

		if i := strings.Index(rest, delimiter); i >= 0 {
			dir := prefix + rest[:i+len(delimiter)]
			if len(entries) == 0 || entries[len(entries)-1].key != dir {
				entries = append(entries, entry{key: dir, isDir: true})
			}

			continue
		}

		entries = append(entries, entry{key: key})
	}

	start := 0
	if token != "" {
		start, _ = strconv.Atoi(token)
	}

	end := min(start+statsPageSize, len(entries))
	out := &s3.ListObjectsV2Output{}

	for _, e := range entries[start:end] {
		if e.isDir {
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(e.key)})
		} else {
			out.Contents = append(out.Contents, types.Object{Key: aws.String(e.key)})
		}
	}

	if end < len(entries) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(strconv.Itoa(end))
	}

	return out
}

func TestLister_GetStatistics(t *testing.T) {
	t.Parallel()

	queryManifests := statsKey + "/ns/bar/query-stream/manifest/"
	changeManifests := statsKey + "/ns/bar/change-stream/" + statsNode + "/manifest/"

	tests := []struct {
		name    string
		objects map[string]string
		want    models.Statistics
	}{
		{
			name: "every count is summed per stream",
			objects: map[string]string{
				statsKey + "/metadata.json":                                             metadataJSON(jobID1),
				statsKey + "/sindex/ns/bar/sindex.json":                                 `{"sindex":[]}`,
				statsKey + "/udf/udf.json":                                              `{"udf":[]}`,
				queryManifests + "1053-0-0000061529140-b4f973.json":                     queryManifestJSON(1, 2, 100, 200),
				queryManifests + "2437-0-0000061529145-0e36ef.json":                     queryManifestJSON(3, 4, 300, 400),
				queryManifests + "3676-0-0000061529149-238ab0.json":                     queryManifestJSON(5, 6, 500, 600),
				statsKey + "/ns/bar/query-stream/data/p1053/0-0000061529085-c4441f.seg": "segment",
				changeManifests + "0-0000062031435-5e58e6.json":                         changeManifestJSON(7, 700),
				statsKey + "/ns/bar/change-stream/" + statsNode + "/data/0-1-a.seg":     "segment",
			},
			want: models.Statistics{
				RecordCount: 9, IncrRecordCount: 12, ChangeRecordCount: 7, ByteCount: 2800, FileCount: 7,
			},
		},
		{
			name: "every namespace and node is summed",
			objects: map[string]string{
				statsKey + "/ns/bar/query-stream/manifest/1-0-1-a.json":          queryManifestJSON(1, 0, 10, 0),
				statsKey + "/ns/baz/query-stream/manifest/1-0-1-a.json":          queryManifestJSON(2, 0, 20, 0),
				statsKey + "/ns/bar/change-stream/node1/manifest/0-1-a.json":     changeManifestJSON(3, 30),
				statsKey + "/ns/bar/change-stream/node2/manifest/0-1-a.json":     changeManifestJSON(4, 40),
				statsKey + "/ns/baz/change-stream/node1/manifest/0-1-a.json":     changeManifestJSON(5, 50),
				statsKey + "/ns/baz/change-stream/node2/manifest/0-1-a.json":     changeManifestJSON(6, 60),
				statsKey + "/ns/baz/change-stream/node3/manifest/0-1-a.json":     changeManifestJSON(7, 70),
				statsKey + "/ns/baz/change-stream/node3/manifest/not-a-manifest": "ignored",
			},
			want: models.Statistics{
				RecordCount: 3, ChangeRecordCount: 25, ByteCount: 280, FileCount: 9,
			},
		},
		{
			name: "a backup without records has zero statistics",
			objects: map[string]string{
				statsKey + "/metadata.json":                           metadataJSON(jobID1),
				statsKey + "/ns/bar/change-stream/" + statsNode + "/": "",
			},
			want: models.Statistics{},
		},
		{
			name:    "a backup that does not exist has zero statistics",
			objects: map[string]string{jobID2 + "/ns/bar/query-stream/manifest/1-0-1-a.json": queryManifestJSON(1, 1, 1, 1)},
			want:    models.Statistics{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := NewLister(mockBucket(t, tt.objects), "bucket", "backups")

			got, err := l.GetStatistics(t.Context(), statsKey)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLister_GetStatistics_ManifestErrors(t *testing.T) {
	t.Parallel()

	manifestKey := statsKey + "/ns/bar/query-stream/manifest/1-0-1-a.json"

	tests := []struct {
		name string
		body string
	}{
		{name: "unparseable manifest", body: `{"num_base_records":`},
		{name: "count of the wrong type", body: `{"num_base_records":"1"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := NewLister(mockBucket(t, map[string]string{manifestKey: tt.body}), "bucket", "")

			_, err := l.GetStatistics(t.Context(), statsKey)
			require.ErrorContains(t, err, "failed to parse manifest "+manifestKey)
		})
	}
}

func TestLister_GetStatistics_S3Errors(t *testing.T) {
	t.Parallel()

	errS3 := errors.New("s3 unavailable")
	manifestKey := statsKey + "/ns/bar/query-stream/manifest/1-0-1-a.json"

	t.Run("list", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).Return(nil, errS3)

		_, err := NewLister(m, "bucket", "").GetStatistics(t.Context(), statsKey)
		require.ErrorIs(t, err, errS3)
	})

	t.Run("get", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return listPage([]string{manifestKey}, aws.ToString(in.Prefix), aws.ToString(in.Delimiter), ""), nil
			})
		m.EXPECT().GetObject(mock.Anything, mock.Anything).Return(nil, errS3)

		_, err := NewLister(m, "bucket", "").GetStatistics(t.Context(), statsKey)
		require.ErrorIs(t, err, errS3)
	})
}
