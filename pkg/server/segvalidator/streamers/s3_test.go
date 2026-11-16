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

package streamers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBucket = "backups"

// fakeBucket answers the two requests this package makes the way S3 does:
// listings are paginated and, given a delimiter, name the directories of a
// level instead of everything below them.
type fakeBucket struct {
	objects map[string][]byte
	// keys are the object keys, sorted, which is the order a listing returns.
	keys []string
	// pageSize is small on purpose, so that every listing of a test is
	// paginated.
	pageSize int

	lists atomic.Int64
	gets  atomic.Int64
}

func newFakeBucket(objects map[string][]byte) *fakeBucket {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return &fakeBucket{objects: objects, keys: keys, pageSize: 3}
}

// listEntry is one line of a listing: an object, or a directory below the
// prefix that was asked for.
type listEntry struct {
	name  string
	size  int64
	isDir bool
}

func (f *fakeBucket) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	f.lists.Add(1)

	if aws.ToString(in.Bucket) != testBucket {
		return nil, &types.NoSuchBucket{}
	}

	entries := f.entries(aws.ToString(in.Prefix), aws.ToString(in.Delimiter))

	// A continuation token is the last name of the page before it.
	if token := aws.ToString(in.ContinuationToken); token != "" {
		for len(entries) > 0 && entries[0].name <= token {
			entries = entries[1:]
		}
	}

	truncated := len(entries) > f.pageSize
	if truncated {
		entries = entries[:f.pageSize]
	}

	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(truncated)}

	for _, entry := range entries {
		if entry.isDir {
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(entry.name)})
			continue
		}

		out.Contents = append(out.Contents, types.Object{Key: aws.String(entry.name), Size: aws.Int64(entry.size)})
	}

	if truncated {
		out.NextContinuationToken = aws.String(entries[len(entries)-1].name)
	}

	return out, nil
}

// entries is what a listing of a prefix returns, in order.
func (f *fakeBucket) entries(prefix, delimiter string) []listEntry {
	var (
		entries []listEntry
		lastDir string
	)

	for _, key := range f.keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		rest := strings.TrimPrefix(key, prefix)

		if i := strings.Index(rest, delimiter); delimiter != "" && i >= 0 {
			if dir := prefix + rest[:i+len(delimiter)]; dir != lastDir {
				entries = append(entries, listEntry{name: dir, isDir: true})
				lastDir = dir
			}

			continue
		}

		entries = append(entries, listEntry{name: key, size: int64(len(f.objects[key]))})
	}

	slices.SortFunc(entries, func(a, b listEntry) int {
		return strings.Compare(a.name, b.name)
	})

	return entries
}

func (f *fakeBucket) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	f.gets.Add(1)

	body, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func TestNewS3Validation(t *testing.T) {
	t.Parallel()

	_, err := NewS3(nil, testBucket, testBackupID)
	require.Error(t, err, "NewS3() with no client succeeded")

	_, err = NewS3(newFakeBucket(nil), "", testBackupID)
	require.Error(t, err, "NewS3() with no bucket succeeded")

	_, err = NewS3(newFakeBucket(nil), testBucket, "")
	require.Error(t, err, "NewS3() with no backup id succeeded")

	s, err := NewS3(newFakeBucket(nil), testBucket, testBackupID)
	require.NoError(t, err)

	assert.Equal(t, testBackupID, s.BackupID())
}

func TestS3Store_ListDirsNamesOneLevel(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 12, 2, 2, 2)
	st := &s3Store{client: newFakeBucket(b.files), bucket: testBucket}

	dirs, err := listDirs(t.Context(), st, path.Join(testBackupID, namespacesDir, testNS))
	require.NoError(t, err)

	sort.Strings(dirs)

	require.Equal(t, []string{string(ChangeStream), string(QueryStream)}, dirs)

	// The listing names the partitions themselves, not the segments below them,
	// however many there are.
	partitions, err := listDirs(t.Context(), st,
		path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir))
	require.NoError(t, err)

	require.Len(t, partitions, 12, "listDirs() partitions")
}

func TestS3Store_ListFilesPaginates(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 4, 5, 0, 0)
	bucket := newFakeBucket(b.files)
	st := &s3Store{client: bucket, bucket: testBucket}

	var found []string

	err := st.listFiles(t.Context(), path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir),
		func(f file) error {
			found = append(found, f.Path)

			return nil
		})
	require.NoError(t, err)

	sort.Strings(found)

	require.Equal(t, b.segments(), found)

	assert.GreaterOrEqual(t, bucket.lists.Load(), int64(2), "want the listing to have been paginated")
}

func TestS3Store_ListFilesStops(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 10, 5, 0, 0)
	bucket := newFakeBucket(b.files)
	st := &s3Store{client: bucket, bucket: testBucket}

	seen := 0

	err := st.listFiles(t.Context(), testBackupID, func(file) error {
		seen++

		return errStopListing
	})
	require.NoError(t, err)

	assert.Equal(t, 1, seen, "files seen after stopping the listing")

	assert.Equal(t, int64(1), bucket.lists.Load(), "listing requests after being stopped")
}

func TestS3Store_DirectoryMarkersAreNotFiles(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)
	// A bucket written through a console holds an empty object per directory.
	b.put(path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir)+"/", nil)

	st := &s3Store{client: newFakeBucket(b.files), bucket: testBucket}

	var found []string

	err := st.listFiles(t.Context(), testBackupID, func(f file) error {
		found = append(found, f.Path)

		return nil
	})
	require.NoError(t, err)

	for _, p := range found {
		assert.False(t, strings.HasSuffix(p, "/"), "listFiles() returned the directory marker %q", p)
	}
}

func TestS3Store_MissingObject(t *testing.T) {
	t.Parallel()

	st := &s3Store{client: newFakeBucket(map[string][]byte{}), bucket: testBucket}

	_, err := st.open(t.Context(), "no/such/segment.seg")
	require.ErrorIs(t, err, ErrSegmentMissing)
}

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	assert.False(t, isNotFound(nil), "isNotFound(nil)")
	assert.False(t, isNotFound(errors.New("connection reset")), "isNotFound() for an error that is not an API error")
	assert.True(t, isNotFound(&types.NoSuchKey{}), "isNotFound(NoSuchKey)")
	assert.False(t, isNotFound(&types.NoSuchBucket{}), "isNotFound(NoSuchBucket), want a missing bucket to be a failure")
}

func TestS3Store_ListingFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)
	// A bucket the client cannot reach is a failure of the listing, not an
	// empty backup.
	st := &s3Store{client: newFakeBucket(b.files), bucket: "no-such-bucket"}

	require.Error(t, st.listLevel(t.Context(), testBackupID, func(levelEntry) error { return nil }),
		"listLevel() of an unreachable bucket succeeded")

	assert.Error(t, st.listFiles(t.Context(), testBackupID, func(file) error { return nil }),
		"listFiles() of an unreachable bucket succeeded")
}

func TestS3Store_GetFails(t *testing.T) {
	t.Parallel()

	st := &s3Store{client: &failingBucket{err: errBucket}, bucket: testBucket}

	_, err := st.open(t.Context(), path.Join(testBackupID, "manifest.json"))

	require.ErrorIs(t, err, errBucket, "want the failure of the request")

	// A request that failed says nothing about whether the object is there.
	assert.NotErrorIs(t, err, ErrSegmentMissing, "want a failed request")
}

// errBucket is what a bucket that will not answer reports.
var errBucket = errors.New("bucket is unreachable")

// failingBucket fails every request made of it.
type failingBucket struct {
	err error
}

func (f *failingBucket) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return nil, f.err
}

func (f *failingBucket) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return nil, f.err
}

func TestS3Store_ListLevelStopsOnTheCaller(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 4, 1, 0, 0)
	st := &s3Store{client: newFakeBucket(b.files), bucket: testBucket}
	dir := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir)

	seen := 0

	err := st.listLevel(t.Context(), dir, func(levelEntry) error {
		seen++

		return errStopListing
	})
	require.NoError(t, err, "want the listing to stop without failing")

	assert.Equal(t, 1, seen, "listLevel() handed over more partitions after the first one stopped it")

	errCaller := errors.New("caller failed")

	err = st.listLevel(t.Context(), dir, func(levelEntry) error {
		return errCaller
	})
	assert.ErrorIs(t, err, errCaller, "want the failure of the caller")
}

func TestS3Store_ListLevelSkipsDirectoryMarkers(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)
	dir := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir)

	// A bucket written through a console holds an empty object per directory,
	// and one written by something careless holds a doubled separator, which
	// names a directory with no name at all.
	b.put(dir+"/", nil)
	b.put(dir+"//stray", nil)

	st := &s3Store{client: newFakeBucket(b.files), bucket: testBucket}

	var found []levelEntry

	err := st.listLevel(t.Context(), dir, func(e levelEntry) error {
		found = append(found, e)

		return nil
	})
	require.NoError(t, err)

	for _, e := range found {
		assert.False(t, e.Name == "" || strings.HasSuffix(e.Path, "/"),
			"listLevel() returned %+v, want neither a marker nor a nameless directory", e)
	}
}
