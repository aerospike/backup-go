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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/internal/util/collections"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/aerospike/backup-go/pkg/server/internal/manifestname"
	"github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	namespacesDir   = "ns"
	queryStreamDir  = "query-stream"
	changeStreamDir = "change-stream"
	manifestDir     = "manifest"
	sindexDir       = "sindex"
	sindexFileName  = "sindex.json"
	udfDir          = "udf"
	udfFileName     = "udf.json"

	// maxObjectSize caps every object GetStatistics reads. A query stream
	// manifest lists the segments of one partition, so it grows with the
	// partition, and udf.json holds the source of every UDF module.
	maxObjectSize = 64 << 20
)

// segmentEntry is a segment as a manifest lists it.
type segmentEntry struct {
	Size uint64 `json:"size"`
}

// queryManifest holds the counts of a query stream manifest.
type queryManifest struct {
	// NumBaseRecords is cumulative across the owners of the partition.
	NumBaseRecords uint64 `json:"num_base_records"`
	// NumIncrRecords is this owner's incremental round only.
	NumIncrRecords uint64         `json:"num_incr_records"`
	Segments       []segmentEntry `json:"segments"`
}

// changeManifest holds the counts of a change stream manifest.
type changeManifest struct {
	NumRecords uint64         `json:"num_records"`
	Segments   []segmentEntry `json:"segments"`
}

// sindexFile holds the secondary index definitions of one namespace, one entry
// per index; deleted indexes are left out.
type sindexFile struct {
	// SIndexes are only counted, so their fields are not decoded.
	SIndexes []struct{} `json:"sindex"`
}

// udfFile holds the UDF modules of the cluster, one entry per module.
type udfFile struct {
	// UDFs are only counted, so their fields are not decoded.
	UDFs []struct{} `json:"udf"`
}

// manifestRef is a manifest a restore applies, which GetStatistics reads.
type manifestRef struct {
	key    string
	change bool
	// newest marks the newest query stream manifest of its partition, whose
	// base count is the partition's.
	newest bool
}

// manifestDirRef is a directory of manifests of either stream.
type manifestDirRef struct {
	key    string
	change bool
}

// GetStatistics sums the record, byte and file counts of one backup from the
// manifests the server wrote for it, and counts the secondary indexes and UDFs
// it holds. Like GetMetadata, it takes the job id the backup was started with,
// not a storage path, and looks it up under the lister's prefix; anything else
// fails with [ErrInvalidBackupID]. It reads no record data, so its cost grows with the number of
// manifests, about one per partition plus one per node, and not with the size
// of the backup.
//
// A server side backup lays out its files as follows:
//
//	<backup id>/metadata.json
//	<backup id>/sindex/ns/<namespace>/sindex.json
//	<backup id>/udf/udf.json
//	<backup id>/ns/<namespace>/query-stream/manifest/<partition>-<regime>-<time>-<uuid>.json
//	<backup id>/ns/<namespace>/query-stream/data/p<partition>/<...>.seg
//	<backup id>/ns/<namespace>/change-stream/<node id>/manifest/<regime>-<time>-<uuid>.json
//	<backup id>/ns/<namespace>/change-stream/<node id>/data/<...>.seg
//
// metadata.json records no counts. GetStatistics counts what a restore of the
// backup applies, and picks the manifests the way restore does:
//   - The query stream has a manifest per partition that holds records, and
//     none for an empty partition. A partition that changed owner while the
//     backup ran, as a node left or joined, has one more manifest per owner,
//     named under a higher regime, and restore applies the segments of all of
//     them. Each lists only the segments its owner wrote.
//   - The change stream has a manifest per node. A node whose change stream
//     restarted after a failure writes a new manifest under a higher regime, and
//     restore applies only the newest: the incremental round reads again what
//     the older ones held.
//   - Restore skips a manifest whose name it cannot parse, and so does
//     GetStatistics.
//
// A query stream manifest counts the records of the two rounds the server scans
// its partition in:
//   - num_base_records for the base round, which reads every record of the
//     partition once. It is cumulative: a new owner starts from the count of
//     the newest manifest before it.
//   - num_incr_records for the incremental round, a second full scan that keeps
//     the records updated at or after the backup start less the cluster clock
//     skew, a fixed 20 seconds for an AP namespace. It counts its owner's round
//     only.
//
// A change stream manifest counts in num_records every write the node took
// while the backup ran, across all partitions.
//
// ByteCount and FileCount are exact: they sum the sizes and count the segments
// the manifests restore applies list, of both rounds and of the change stream.
// Segments of a superseded change stream manifest that are still stored are not
// counted.
//
// RecordCount is an estimate: it is the base count of the newest manifest of
// each partition. The server does not deduplicate the rounds and the change
// stream against each other, restore does, so summing them counts a record once
// per round and once per write that caught it. Any record updated within the
// clock skew before the backup started is in both rounds, so the sum doubles
// the count of a backup taken right after its data was loaded. The base round
// holds each record once, which makes it exact for a backup no write overlaps,
// and close otherwise:
//   - it misses the records inserted after the base round scanned their
//     partition;
//   - it still counts the records deleted after the base round scanned them;
//   - the server counts a base round that resumed after a failure slightly high.
//
// IndexCount and UDFCount are exact. The principal node writes sindex.json, the
// definitions of the secondary indexes of the namespace, and udf.json, every UDF
// module of the cluster, when the backup finishes. A backup taken with
// no-indexes or no-udfs, or one that did not finish, lacks the file, and its
// count is zero.
//
// IncrRecordCount and ChangeRecordCount report the other two sources as they
// are, summed over the manifests restore applies. RecordCount + IncrRecordCount
// + ChangeRecordCount is an upper bound on the distinct records of the backup,
// and the change stream count tells how many writes the backup overlapped.
//
// Counting distinct records exactly needs reading every segment, which does not
// scale to large backups.
//
// A backup that does not exist has no manifests, and its statistics are zero.
func (l *Lister) GetStatistics(ctx context.Context, backupID string) (models.Statistics, error) {
	// A job id holds no slash, so the keys cannot leave the prefix.
	if !asinfo.IsJobID(backupID) {
		return models.Statistics{}, fmt.Errorf("%w: %q", ErrInvalidBackupID, backupID)
	}

	backupKey := objectKey(l.prefix, backupID)

	manifests, sindexes, err := l.listStatisticsObjects(ctx, backupKey)
	if err != nil {
		return models.Statistics{}, fmt.Errorf("failed to list backup %s: %w", backupID, err)
	}

	type countFunc func(context.Context) (models.Statistics, error)

	counts := make([]countFunc, 0, len(manifests)+len(sindexes)+1)
	for _, m := range manifests {
		counts = append(counts, func(ctx context.Context) (models.Statistics, error) {
			return l.manifestStatistics(ctx, m)
		})
	}

	for _, key := range sindexes {
		counts = append(counts, func(ctx context.Context) (models.Statistics, error) {
			return l.sindexStatistics(ctx, key)
		})
	}

	udfKey := objectKey(backupKey, udfDir, udfFileName)

	counts = append(counts, func(ctx context.Context) (models.Statistics, error) {
		return l.udfStatistics(ctx, udfKey)
	})

	perObject, err := collections.ParallelMap(ctx, l.concurrency, counts,
		func(ctx context.Context, count countFunc) (models.Statistics, error) {
			return count(ctx)
		})
	if err != nil {
		return models.Statistics{}, err
	}

	var total models.Statistics
	for _, stats := range perObject {
		total.Add(stats)
	}

	return total, nil
}

// listStatisticsObjects returns the manifests restore applies of every
// namespace of the backup whose directory is backupKey, and the keys of its
// sindex.json files.
// It lists level by level, each level's directories concurrently.
func (l *Lister) listStatisticsObjects(
	ctx context.Context, backupKey string,
) (manifests []manifestRef, sindexes []string, err error) {
	roots := []string{objectKey(backupKey, namespacesDir), objectKey(backupKey, sindexDir, namespacesDir)}

	rootDirs, err := collections.ParallelMap(ctx, l.concurrency, roots, l.listSubdirs)
	if err != nil {
		return nil, nil, err
	}

	namespaces := rootDirs[0]
	for _, namespace := range rootDirs[1] {
		sindexes = append(sindexes, objectKey(namespace, sindexFileName))
	}

	nodes, err := collections.ParallelMap(ctx, l.concurrency, namespaces,
		func(ctx context.Context, namespace string) ([]string, error) {
			return l.listSubdirs(ctx, objectKey(namespace, changeStreamDir))
		})
	if err != nil {
		return nil, nil, err
	}

	var dirs []manifestDirRef

	for i, namespace := range namespaces {
		dirs = append(dirs, manifestDirRef{key: objectKey(namespace, queryStreamDir, manifestDir)})
		for _, node := range nodes[i] {
			dirs = append(dirs, manifestDirRef{key: objectKey(node, manifestDir), change: true})
		}
	}

	files, err := collections.ParallelMap(ctx, l.concurrency, dirs,
		func(ctx context.Context, dir manifestDirRef) ([]string, error) {
			_, files, err := l.listDir(ctx, dir.key)
			return files, err
		})
	if err != nil {
		return nil, nil, err
	}

	for i, dir := range dirs {
		if dir.change {
			if m, ok := newestChangeManifest(files[i]); ok {
				manifests = append(manifests, m)
			}

			continue
		}

		manifests = append(manifests, queryManifests(files[i])...)
	}

	return manifests, sindexes, nil
}

// queryManifests returns the query stream manifests among keys, the files of
// one namespace's manifest directory, that restore applies: every one it can
// parse the name of. The newest of each partition is marked.
func queryManifests(keys []string) []manifestRef {
	var (
		refs   = make([]manifestRef, 0, len(keys))
		names  = make([]manifestname.Name, 0, len(keys))
		newest = make(map[int]int)
	)

	for _, key := range keys {
		name, ok := manifestname.ParseQuery(key)
		if !ok {
			continue
		}

		refs = append(refs, manifestRef{key: key})
		names = append(names, name)

		// Like the server, the first of two equally new manifests wins.
		if i, seen := newest[name.Partition]; !seen || name.Newer(names[i]) {
			newest[name.Partition] = len(refs) - 1
		}
	}

	for _, i := range newest {
		refs[i].newest = true
	}

	return refs
}

// newestChangeManifest returns the change stream manifest among keys, the
// files of one node's manifest directory, that restore applies: the newest one
// it can parse the name of. ok is false when there is none.
func newestChangeManifest(keys []string) (ref manifestRef, ok bool) {
	var newest manifestname.Name

	for _, key := range keys {
		name, parsed := manifestname.ParseChange(key)
		if !parsed || ok && !name.Newer(newest) {
			continue
		}

		ref, newest, ok = manifestRef{key: key, change: true}, name, true
	}

	return ref, ok
}

// manifestStatistics reads the counts of the manifest m. The manifest was
// listed, so a missing one is an error.
func (l *Lister) manifestStatistics(ctx context.Context, m manifestRef) (models.Statistics, error) {
	data, found, err := l.fetch(ctx, m.key, maxObjectSize)
	switch {
	case err != nil:
		return models.Statistics{}, err
	case !found:
		return models.Statistics{}, fmt.Errorf("%w: manifest %s", errclass.ErrNotFound, m.key)
	}

	if m.change {
		var manifest changeManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return models.Statistics{}, fmt.Errorf("failed to parse %s: %w", m.key, err)
		}

		stats := segmentStatistics(manifest.Segments)
		stats.ChangeRecordCount = manifest.NumRecords

		return stats, nil
	}

	var manifest queryManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return models.Statistics{}, fmt.Errorf("failed to parse %s: %w", m.key, err)
	}

	stats := segmentStatistics(manifest.Segments)
	stats.IncrRecordCount = manifest.NumIncrRecords

	if m.newest {
		stats.RecordCount = manifest.NumBaseRecords
	}

	return stats, nil
}

// segmentStatistics counts segments and sums their sizes.
func segmentStatistics(segments []segmentEntry) models.Statistics {
	stats := models.Statistics{FileCount: uint64(len(segments))}
	for _, segment := range segments {
		stats.ByteCount += segment.Size
	}

	return stats
}

// sindexStatistics counts the secondary indexes of the sindex.json at key. A
// missing file counts as none.
func (l *Lister) sindexStatistics(ctx context.Context, key string) (models.Statistics, error) {
	data, found, err := l.fetch(ctx, key, maxObjectSize)
	if err != nil || !found {
		return models.Statistics{}, err
	}

	var file sindexFile
	if err := json.Unmarshal(data, &file); err != nil {
		return models.Statistics{}, fmt.Errorf("failed to parse %s: %w", key, err)
	}

	return models.Statistics{IndexCount: uint64(len(file.SIndexes))}, nil
}

// udfStatistics counts the UDF modules of the udf.json at key. A missing file
// counts as none.
func (l *Lister) udfStatistics(ctx context.Context, key string) (models.Statistics, error) {
	data, found, err := l.fetch(ctx, key, maxObjectSize)
	if err != nil || !found {
		return models.Statistics{}, err
	}

	var file udfFile
	if err := json.Unmarshal(data, &file); err != nil {
		return models.Statistics{}, fmt.Errorf("failed to parse %s: %w", key, err)
	}

	return models.Statistics{UDFCount: uint64(len(file.UDFs))}, nil
}

// listSubdirs returns the keys of the directories directly under dir, without
// a trailing slash.
func (l *Lister) listSubdirs(ctx context.Context, dir string) ([]string, error) {
	dirs, _, err := l.listDir(ctx, dir)
	return dirs, err
}

// listDir returns the keys of the directories, without a trailing slash, and of
// the files directly under dir.
func (l *Lister) listDir(ctx context.Context, dir string) (dirs, files []string, err error) {
	prefix := dir + "/"

	pg := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(l.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list %s: %w", prefix, err)
		}

		for _, cp := range page.CommonPrefixes {
			dirs = append(dirs, strings.TrimSuffix(aws.ToString(cp.Prefix), "/"))
		}

		for _, obj := range page.Contents {
			files = append(files, aws.ToString(obj.Key))
		}
	}

	return dirs, files, nil
}
