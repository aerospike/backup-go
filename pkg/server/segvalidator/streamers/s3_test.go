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

	if _, err := NewS3(nil, testBucket, testBackupID); err == nil {
		t.Error("NewS3() with no client succeeded, want an error")
	}

	if _, err := NewS3(newFakeBucket(nil), "", testBackupID); err == nil {
		t.Error("NewS3() with no bucket succeeded, want an error")
	}

	if _, err := NewS3(newFakeBucket(nil), testBucket, ""); err == nil {
		t.Error("NewS3() with no backup id succeeded, want an error")
	}

	s, err := NewS3(newFakeBucket(nil), testBucket, testBackupID)
	if err != nil {
		t.Fatalf("NewS3() error = %v", err)
	}

	if s.BackupID() != testBackupID {
		t.Errorf("BackupID() = %q, want %q", s.BackupID(), testBackupID)
	}
}

func TestS3Store_ListDirsNamesOneLevel(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 12, 2, 2, 2)
	st := &s3Store{client: newFakeBucket(b.files), bucket: testBucket}

	dirs, err := listDirs(t.Context(), st, path.Join(testBackupID, namespacesDir, testNS))
	if err != nil {
		t.Fatalf("listDirs() error = %v", err)
	}

	sort.Strings(dirs)

	if want := []string{string(ChangeStream), string(QueryStream)}; !slices.Equal(dirs, want) {
		t.Fatalf("listDirs() = %v, want %v", dirs, want)
	}

	// The listing names the partitions themselves, not the segments below them,
	// however many there are.
	partitions, err := listDirs(t.Context(), st,
		path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir))
	if err != nil {
		t.Fatalf("listDirs() error = %v", err)
	}

	if len(partitions) != 12 {
		t.Fatalf("listDirs() found %d partitions, want 12: %v", len(partitions), partitions)
	}
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
	if err != nil {
		t.Fatalf("listFiles() error = %v", err)
	}

	sort.Strings(found)

	if want := b.segments(); !slices.Equal(found, want) {
		t.Fatalf("listFiles() found %d files, want %d", len(found), len(want))
	}

	if bucket.lists.Load() < 2 {
		t.Errorf("listing took %d requests, want it to have been paginated", bucket.lists.Load())
	}
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
	if err != nil {
		t.Fatalf("listFiles() error = %v", err)
	}

	if seen != 1 {
		t.Errorf("saw %d files after stopping the listing, want 1", seen)
	}

	if bucket.lists.Load() != 1 {
		t.Errorf("listing took %d requests after being stopped, want 1", bucket.lists.Load())
	}
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
	if err != nil {
		t.Fatalf("listFiles() error = %v", err)
	}

	for _, p := range found {
		if strings.HasSuffix(p, "/") {
			t.Errorf("listFiles() returned the directory marker %q", p)
		}
	}
}

func TestS3Store_MissingObject(t *testing.T) {
	t.Parallel()

	st := &s3Store{client: newFakeBucket(map[string][]byte{}), bucket: testBucket}

	if _, err := st.open(t.Context(), "no/such/segment.seg"); !errors.Is(err, ErrSegmentMissing) {
		t.Fatalf("open() error = %v, want ErrSegmentMissing", err)
	}
}

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	if isNotFound(nil) {
		t.Error("isNotFound(nil) = true, want false")
	}

	if isNotFound(errors.New("connection reset")) {
		t.Error("isNotFound() = true for an error that is not an API error")
	}

	if !isNotFound(&types.NoSuchKey{}) {
		t.Error("isNotFound(NoSuchKey) = false, want true")
	}

	if isNotFound(&types.NoSuchBucket{}) {
		t.Error("isNotFound(NoSuchBucket) = true, want a missing bucket to be a failure")
	}
}

func TestS3Store_ListingFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)
	// A bucket the client cannot reach is a failure of the listing, not an
	// empty backup.
	st := &s3Store{client: newFakeBucket(b.files), bucket: "no-such-bucket"}

	if err := st.listLevel(t.Context(), testBackupID, func(levelEntry) error { return nil }); err == nil {
		t.Error("listLevel() of an unreachable bucket succeeded, want an error")
	}

	if err := st.listFiles(t.Context(), testBackupID, func(file) error { return nil }); err == nil {
		t.Error("listFiles() of an unreachable bucket succeeded, want an error")
	}
}

func TestS3Store_GetFails(t *testing.T) {
	t.Parallel()

	st := &s3Store{client: &failingBucket{err: errBucket}, bucket: testBucket}

	_, err := st.open(t.Context(), path.Join(testBackupID, "manifest.json"))

	if !errors.Is(err, errBucket) {
		t.Fatalf("open() error = %v, want the failure of the request", err)
	}

	// A request that failed says nothing about whether the object is there.
	if errors.Is(err, ErrSegmentMissing) {
		t.Error("open() error = ErrSegmentMissing, want a failed request")
	}
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
	if err != nil {
		t.Fatalf("listLevel() error = %v, want the listing to stop without failing", err)
	}

	if seen != 1 {
		t.Errorf("listLevel() handed over %d partitions after the first one stopped it, want 1", seen)
	}

	errCaller := errors.New("caller failed")

	if err := st.listLevel(t.Context(), dir, func(levelEntry) error {
		return errCaller
	}); !errors.Is(err, errCaller) {
		t.Errorf("listLevel() error = %v, want the failure of the caller", err)
	}
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

	if err := st.listLevel(t.Context(), dir, func(e levelEntry) error {
		found = append(found, e)

		return nil
	}); err != nil {
		t.Fatalf("listLevel() error = %v", err)
	}

	for _, e := range found {
		if e.Name == "" || strings.HasSuffix(e.Path, "/") {
			t.Errorf("listLevel() returned %+v, want neither a marker nor a nameless directory", e)
		}
	}
}
