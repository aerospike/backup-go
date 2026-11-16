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
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aerospike/backup-go/errclass"
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
	// statsPrefix is the prefix of the lister the tests read jobID1 with, and
	// statsKey the directory of jobID1 under it.
	statsPrefix = "backups"
	statsKey    = statsPrefix + "/" + jobID1
	statsNode   = "BB9D5F76B77F4A6"
	// statsPageSize keeps listing pages small, so a listing spans several pages.
	statsPageSize = 2
)

// queryManifestJSON is a query stream manifest of a partition with base and
// incr counts and a segment of each size. Its byte counts disagree with the
// segment sizes, which are what is counted.
func queryManifestJSON(baseRecords, incrRecords int, segmentSizes ...int) string {
	return `{"backup_id":"` + jobID1 + `","partition_id":1053,"partition_complete":true,` +
		`"num_base_records":` + strconv.Itoa(baseRecords) + `,"num_incr_records":` + strconv.Itoa(incrRecords) +
		`,"num_base_bytes":999999,"num_incr_bytes":999999,"segments":` + segmentsJSON(segmentSizes) + `}`
}

// changeManifestJSON is a change stream manifest of a node with a segment of
// each size.
func changeManifestJSON(records int, segmentSizes ...int) string {
	return `{"backup_id":"` + jobID1 + `","lane":"change","num_records":` + strconv.Itoa(records) +
		`,"num_bytes":999999,"segments":` + segmentsJSON(segmentSizes) + `}`
}

// segmentsJSON is the segment list of a manifest, a segment of each size.
func segmentsJSON(sizes []int) string {
	entries := make([]string, len(sizes))
	for i, size := range sizes {
		entries[i] = `{"segment_name":"` + strconv.Itoa(i) + `.seg","size":` + strconv.Itoa(size) +
			`,"checksum":"1a2b3c4d","record_count":1}`
	}

	return "[" + strings.Join(entries, ",") + "]"
}

// queryManifestName is the name of a query stream manifest of partition pid.
func queryManifestName(pid, regime, timestamp int) string {
	return fmt.Sprintf("%d-%d-%013d-b4f973.json", pid, regime, timestamp)
}

// changeManifestName is the name of a change stream manifest.
func changeManifestName(regime, timestamp int) string {
	return fmt.Sprintf("%d-%013d-5e58e6.json", regime, timestamp)
}

// sindexJSON is a sindex.json of namespace bar, as the server writes it, with
// one entry per index.
func sindexJSON(indexes ...string) string {
	entries := make([]string, len(indexes))
	for i, name := range indexes {
		entries[i] = `{"key":"bar|users|` + name + `|.|I","value":"` + name + `"}`
	}

	return `{"backup_id":"` + jobID1 + `","namespace":"bar","format_version":2,"sindex":[` +
		strings.Join(entries, ",") + `]}`
}

// udfJSON is a udf.json, as the server writes it, with one entry per module.
func udfJSON(modules ...string) string {
	entries := make([]string, len(modules))
	for i, name := range modules {
		entries[i] = `{"key":"` + name + `","value":"{\"content64\": \"cmV0dXJu\", \"type\": \"LUA\", ` +
			`\"name\": \"` + name + `\"}"}`
	}

	return `{"backup_id":"` + jobID1 + `","format_version":2,"udf":[` + strings.Join(entries, ",") + `]}`
}

// finished adds the metadata.json of a finished backup jobID1 to objects.
func finished(objects map[string]string) map[string]string {
	out := map[string]string{statsKey + "/metadata.json": metadataJSON(jobID1)}
	maps.Copy(out, objects)

	return out
}

// metadataOr serves the metadata.json of jobID1 and fails every other read
// with err.
func metadataOr(err error) func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		if aws.ToString(in.Key) == statsKey+"/metadata.json" {
			return objectBody(metadataJSON(jobID1)), nil
		}

		return nil, err
	}
}

// listOrFail lists key as S3 would, and fails with err the listing of every
// prefix that holds marker.
func listOrFail(
	key, marker string, err error,
) func(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
		if strings.Contains(aws.ToString(in.Prefix), marker) {
			return nil, err
		}

		return listPage([]string{key}, aws.ToString(in.Prefix), aws.ToString(in.Delimiter), ""), nil
	}
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
				statsKey + "/sindex/ns/bar/sindex.json":                                 sindexJSON("age", "email"),
				statsKey + "/udf/udf.json":                                              udfJSON("a.lua", "b.lua", "c.lua"),
				queryManifests + queryManifestName(1053, 0, 61529140):                   queryManifestJSON(1, 2, 100, 200),
				queryManifests + queryManifestName(2437, 0, 61529145):                   queryManifestJSON(3, 4, 300, 400),
				queryManifests + queryManifestName(3676, 0, 61529149):                   queryManifestJSON(5, 6, 500, 600),
				statsKey + "/ns/bar/query-stream/data/p1053/0-0000061529085-c4441f.seg": "segment",
				changeManifests + changeManifestName(0, 62031435):                       changeManifestJSON(7, 700),
				statsKey + "/ns/bar/change-stream/" + statsNode + "/data/0-1-a.seg":     "segment",
			},
			want: models.Statistics{
				RecordCount: 9, IncrRecordCount: 12, ChangeRecordCount: 7, ByteCount: 2800, FileCount: 7,
				IndexCount: 2, UDFCount: 3,
			},
		},
		{
			name: "every namespace and node is summed",
			objects: map[string]string{
				statsKey + "/ns/bar/query-stream/manifest/" + queryManifestName(1, 0, 1):      queryManifestJSON(1, 0, 10),
				statsKey + "/ns/baz/query-stream/manifest/" + queryManifestName(1, 0, 1):      queryManifestJSON(2, 0, 20),
				statsKey + "/ns/bar/change-stream/node1/manifest/" + changeManifestName(0, 1): changeManifestJSON(3, 30),
				statsKey + "/ns/bar/change-stream/node2/manifest/" + changeManifestName(0, 1): changeManifestJSON(4, 40),
				statsKey + "/ns/baz/change-stream/node1/manifest/" + changeManifestName(0, 1): changeManifestJSON(5, 50),
				statsKey + "/ns/baz/change-stream/node2/manifest/" + changeManifestName(0, 1): changeManifestJSON(6, 60),
				statsKey + "/ns/baz/change-stream/node3/manifest/" + changeManifestName(0, 1): changeManifestJSON(7, 70),
				statsKey + "/ns/baz/change-stream/node3/manifest/not-a-manifest":              "ignored",
			},
			want: models.Statistics{
				RecordCount: 3, ChangeRecordCount: 25, ByteCount: 280, FileCount: 7,
			},
		},
		{
			name: "a partition that changed owner takes its base count from its newest manifest",
			objects: map[string]string{
				// The new owner starts from the old owner's base count of 1000.
				queryManifests + queryManifestName(17, 3, 50): queryManifestJSON(1000, 1, 100, 200),
				queryManifests + queryManifestName(17, 4, 10): queryManifestJSON(1500, 2, 50, 60),
				// A third owner under the same regime, later.
				queryManifests + queryManifestName(17, 4, 20): queryManifestJSON(1600, 3, 5),
				queryManifests + queryManifestName(18, 1, 1):  queryManifestJSON(7, 0, 1),
			},
			want: models.Statistics{
				RecordCount: 1607, IncrRecordCount: 6, ByteCount: 416, FileCount: 6,
			},
		},
		{
			name: "only the newest change stream manifest of a node is counted",
			objects: map[string]string{
				changeManifests + changeManifestName(5, 10):                                   changeManifestJSON(3, 30),
				changeManifests + changeManifestName(4, 90):                                   changeManifestJSON(10, 100, 100),
				changeManifests + changeManifestName(5, 9):                                    changeManifestJSON(20, 200),
				statsKey + "/ns/bar/change-stream/node2/manifest/" + changeManifestName(1, 1): changeManifestJSON(1, 1),
			},
			want: models.Statistics{ChangeRecordCount: 4, ByteCount: 31, FileCount: 2},
		},
		{
			name: "manifests restore cannot name are skipped",
			objects: map[string]string{
				queryManifests + queryManifestName(17, 1, 1):     queryManifestJSON(1, 0, 10),
				queryManifests + "17-foo.json":                   `{"num_base_records":":-("}`,
				queryManifests + queryManifestName(4096, 1, 1):   `{"num_base_records":":-("}`,
				queryManifests + changeManifestName(1, 1):        `{"num_base_records":":-("}`,
				changeManifests + changeManifestName(1, 1):       changeManifestJSON(2, 20),
				changeManifests + "x-1-5e58e6.json":              `{"num_records":":-("}`,
				changeManifests + "manifest.json":                `{"num_records":":-("}`,
				changeManifests + changeManifestName(9, 9) + "x": `{"num_records":":-("}`,
			},
			want: models.Statistics{RecordCount: 1, ChangeRecordCount: 2, ByteCount: 30, FileCount: 2},
		},
		{
			name: "secondary indexes are summed over namespaces",
			objects: map[string]string{
				statsKey + "/sindex/ns/bar/sindex.json": sindexJSON("a", "b", "c"),
				statsKey + "/sindex/ns/baz/sindex.json": sindexJSON("d"),
				statsKey + "/sindex/ns/qux/sindex.json": sindexJSON(),
				statsKey + "/sindex/ns/quux/other.json": "ignored",
				statsKey + "/udf/udf.json":              udfJSON("a.lua"),
			},
			want: models.Statistics{IndexCount: 4, UDFCount: 1},
		},
		{
			name: "a backup without sindex.json and udf.json has no indexes and udfs",
			objects: map[string]string{
				statsKey + "/metadata.json": metadataJSON(jobID1),
				statsKey + "/ns/bar/query-stream/manifest/" + queryManifestName(1, 0, 1): queryManifestJSON(1, 0, 4, 6),
			},
			want: models.Statistics{RecordCount: 1, ByteCount: 10, FileCount: 2},
		},
		{
			name: "a backup without records has zero statistics",
			objects: map[string]string{
				statsKey + "/metadata.json":                           metadataJSON(jobID1),
				statsKey + "/ns/bar/change-stream/" + statsNode + "/": "",
			},
			want: models.Statistics{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := NewLister(mockBucket(t, finished(tt.objects)), "bucket", statsPrefix)

			got, err := l.GetStatistics(t.Context(), jobID1)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLister_GetStatistics_ParseErrors(t *testing.T) {
	t.Parallel()

	manifestKey := statsKey + "/ns/bar/query-stream/manifest/" + queryManifestName(1, 0, 1)
	changeKey := statsKey + "/ns/bar/change-stream/" + statsNode + "/manifest/" + changeManifestName(0, 1)
	sindexKey := statsKey + "/sindex/ns/bar/sindex.json"
	udfKey := statsKey + "/udf/udf.json"

	tests := []struct {
		name string
		key  string
		body string
	}{
		{name: "unparseable manifest", key: manifestKey, body: `{"num_base_records":`},
		{name: "count of the wrong type", key: manifestKey, body: `{"num_base_records":"1"}`},
		{name: "segment size of the wrong type", key: manifestKey, body: `{"segments":[{"size":-1}]}`},
		{name: "unparseable change manifest", key: changeKey, body: `{"num_records":`},
		{name: "unparseable sindex.json", key: sindexKey, body: `{"sindex":[`},
		{name: "sindex of the wrong type", key: sindexKey, body: `{"sindex":{}}`},
		{name: "unparseable udf.json", key: udfKey, body: `{"udf":`},
		{name: "udf of the wrong type", key: udfKey, body: `{"udf":"a.lua"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := NewLister(mockBucket(t, finished(map[string]string{tt.key: tt.body})), "bucket", statsPrefix)

			_, err := l.GetStatistics(t.Context(), jobID1)
			require.ErrorContains(t, err, "failed to parse "+tt.key)
		})
	}
}

func TestLister_GetStatistics_S3Errors(t *testing.T) {
	t.Parallel()

	errS3 := errors.New("s3 unavailable")
	manifestKey := statsKey + "/ns/bar/query-stream/manifest/" + queryManifestName(1, 0, 1)

	t.Run("list", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(metadataOr(nil))
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).Return(nil, errS3)

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
	})

	t.Run("get metadata.json", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().GetObject(mock.Anything, mock.Anything).Return(nil, errS3)

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
		require.ErrorContains(t, err, "metadata")
	})

	t.Run("get manifest", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return listPage([]string{manifestKey}, aws.ToString(in.Prefix), aws.ToString(in.Delimiter), ""), nil
			})
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(metadataOr(errS3))

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
	})

	t.Run("listed manifest is gone", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return listPage([]string{manifestKey}, aws.ToString(in.Prefix), aws.ToString(in.Delimiter), ""), nil
			})
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(metadataOr(&types.NoSuchKey{}))

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errclass.ErrNotFound)
		require.ErrorContains(t, err, manifestKey)

		// The S3 error is passed on, not replaced.
		var noSuchKey *types.NoSuchKey
		require.ErrorAs(t, err, &noSuchKey)
	})

	t.Run("list the nodes of a change stream", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(metadataOr(nil))
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			listOrFail(manifestKey, "/change-stream/", errS3))

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
	})

	t.Run("list a manifest directory", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(metadataOr(nil))
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			listOrFail(manifestKey, "/manifest/", errS3))

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
	})

	t.Run("get sindex.json", func(t *testing.T) {
		t.Parallel()

		sindexKey := statsKey + "/sindex/ns/bar/sindex.json"

		m := mocks.NewMockS3API(t)
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return listPage([]string{sindexKey}, aws.ToString(in.Prefix), aws.ToString(in.Delimiter), ""), nil
			})
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				if aws.ToString(in.Key) == sindexKey {
					return nil, errS3
				}

				// udf.json is missing, which is no error.
				return metadataOr(&types.NoSuchKey{})(ctx, in, opts...)
			})

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
		require.ErrorContains(t, err, sindexKey)
	})

	t.Run("get udf.json", func(t *testing.T) {
		t.Parallel()

		m := mocks.NewMockS3API(t)
		m.EXPECT().ListObjectsV2(mock.Anything, mock.Anything, mock.Anything).Return(&s3.ListObjectsV2Output{}, nil)
		m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(metadataOr(errS3))

		_, err := NewLister(m, "bucket", statsPrefix).GetStatistics(t.Context(), jobID1)
		require.ErrorIs(t, err, errS3)
		require.ErrorContains(t, err, statsKey+"/udf/udf.json")
	})
}

func TestLister_GetStatistics_NoMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		objects map[string]string
	}{
		{
			name: "backup that does not exist",
			objects: map[string]string{
				jobID2 + "/ns/bar/query-stream/manifest/" + queryManifestName(1, 0, 1): queryManifestJSON(1, 1, 1),
			},
		},
		{
			name: "unfinished backup",
			objects: map[string]string{
				statsKey + "/ns/bar/query-stream/manifest/" + queryManifestName(1, 0, 1): queryManifestJSON(1, 1, 1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := NewLister(mockBucket(t, tt.objects), "bucket", statsPrefix)

			_, err := l.GetStatistics(t.Context(), jobID1)
			require.ErrorIs(t, err, ErrMetadataNotFound)
		})
	}
}

func TestLister_GetStatistics_InvalidBackupID(t *testing.T) {
	t.Parallel()

	for _, id := range []string{"", statsKey, "../" + jobID1, jobID1 + "/ns"} {
		t.Run(id, func(t *testing.T) {
			t.Parallel()

			// No S3 call is expected: the id is refused first.
			_, err := NewLister(mocks.NewMockS3API(t), "bucket", statsPrefix).GetStatistics(t.Context(), id)
			require.ErrorIs(t, err, ErrInvalidBackupID)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
		})
	}
}

func TestQueryManifests(t *testing.T) {
	t.Parallel()

	const dir = statsKey + "/ns/bar/query-stream/manifest/"

	keys := []string{
		dir + queryManifestName(17, 3, 50),
		dir + queryManifestName(17, 4, 10),
		dir + queryManifestName(17, 4, 20),
		// Same regime and timestamp as the one before: the first listed wins.
		dir + "17-4-0000000000020-ffffff.json",
		dir + queryManifestName(18, 1, 1),
		dir + "17-foo.json",
		dir + changeManifestName(1, 1),
	}

	assert.Equal(t, []manifestRef{
		{key: keys[0]},
		{key: keys[1]},
		{key: keys[2], newest: true},
		{key: keys[3]},
		{key: keys[4], newest: true},
	}, queryManifests(keys))
	assert.Empty(t, queryManifests(nil))
}

func TestNewestChangeManifest(t *testing.T) {
	t.Parallel()

	const dir = statsKey + "/ns/bar/change-stream/" + statsNode + "/manifest/"

	tests := []struct {
		name   string
		keys   []string
		want   string
		wantOK bool
	}{
		{
			name: "higher regime wins over a later timestamp",
			keys: []string{
				dir + changeManifestName(4, 90), dir + changeManifestName(5, 10), dir + changeManifestName(5, 9),
			},
			want:   dir + changeManifestName(5, 10),
			wantOK: true,
		},
		{
			name:   "the first of two equally new manifests wins",
			keys:   []string{dir + "5-0000000000010-aaaaaa.json", dir + "5-0000000000010-bbbbbb.json"},
			want:   dir + "5-0000000000010-aaaaaa.json",
			wantOK: true,
		},
		{
			name:   "names restore cannot parse are skipped",
			keys:   []string{dir + "manifest.json", dir + changeManifestName(1, 1), dir + "x-1-5e58e6.json"},
			want:   dir + changeManifestName(1, 1),
			wantOK: true,
		},
		{name: "no parsable name", keys: []string{dir + "manifest.json"}},
		{name: "no manifest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := newestChangeManifest(tt.keys)
			require.Equal(t, tt.wantOK, ok)

			if ok {
				assert.Equal(t, manifestRef{key: tt.want, change: true}, got)
			}
		})
	}
}

func TestLister_fetch(t *testing.T) {
	t.Parallel()

	const key = "backups/object.json"

	errS3 := errors.New("s3 unavailable")
	errRead := errors.New("connection reset")

	tests := []struct {
		name        string
		out         *s3.GetObjectOutput
		err         error
		want        string
		wantErr     error
		notNotFound bool
	}{
		{name: "shorter than the limit", out: objectBody("abc"), want: "abc"},
		{name: "cut short at the limit", out: objectBody("abcdef"), want: "abcd"},
		{name: "missing object", err: &types.NoSuchKey{}, wantErr: errclass.ErrNotFound},
		{name: "other S3 error", err: errS3, wantErr: errS3, notNotFound: true},
		{
			name:        "failed read",
			out:         &s3.GetObjectOutput{Body: io.NopCloser(io.MultiReader(strings.NewReader("ab"), failingReader{errRead}))},
			wantErr:     errRead,
			notNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := mocks.NewMockS3API(t)
			m.EXPECT().GetObject(mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					require.Equal(t, "bucket", aws.ToString(in.Bucket))
					require.Equal(t, key, aws.ToString(in.Key))

					return tt.out, tt.err
				})

			got, err := NewLister(m, "bucket", "").fetch(t.Context(), key, 4)
			if tt.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, tt.want, string(got))

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
			require.ErrorContains(t, err, key)
			assert.Nil(t, got)

			if tt.notNotFound {
				assert.NotErrorIs(t, err, errclass.ErrNotFound)
			}
		})
	}
}

// failingReader fails every read with err.
type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}
