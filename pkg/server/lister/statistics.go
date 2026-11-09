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
	"io"
	"slices"
	"strings"

	"github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

const (
	namespacesDir   = "ns"
	queryStreamDir  = "query-stream"
	changeStreamDir = "change-stream"
	manifestDir     = "manifest"
	manifestSuffix  = ".json"

	// maxManifestSize caps a manifest read. A query stream manifest lists the
	// segments of one partition, so it grows with the partition.
	maxManifestSize = 64 << 20
)

// streamManifest holds the counts of a manifest of either stream. A query
// stream manifest sets the base and incr counts, a change stream manifest the
// plain ones.
type streamManifest struct {
	NumBaseRecords uint64 `json:"num_base_records"`
	NumIncrRecords uint64 `json:"num_incr_records"`
	NumRecords     uint64 `json:"num_records"`
	NumBaseBytes   uint64 `json:"num_base_bytes"`
	NumIncrBytes   uint64 `json:"num_incr_bytes"`
	NumBytes       uint64 `json:"num_bytes"`
	// Segments are only counted, so their fields are not decoded.
	Segments []struct{} `json:"segments"`
}

func (m *streamManifest) statistics() models.Statistics {
	return models.Statistics{
		RecordCount:       m.NumBaseRecords,
		IncrRecordCount:   m.NumIncrRecords,
		ChangeRecordCount: m.NumRecords,
		ByteCount:         m.NumBaseBytes + m.NumIncrBytes + m.NumBytes,
		FileCount:         uint64(len(m.Segments)),
	}
}

// GetStatistics sums the record, byte and file counts of the backup at
// backupKey, the key of its directory as GetMetadata takes it, from the
// manifests the server wrote for it. It reads no record data, so its cost grows
// with the number of manifests, at most 4096 per namespace plus one per node,
// and not with the size of the backup.
//
// A server side backup lays out its files as follows:
//
//	<backup id>/metadata.json
//	<backup id>/sindex/ns/<namespace>/sindex.json
//	<backup id>/udf/udf.json
//	<backup id>/ns/<namespace>/query-stream/manifest/<partition>-<regime>-<time>-<salt>.json
//	<backup id>/ns/<namespace>/query-stream/data/p<partition>/<...>.seg
//	<backup id>/ns/<namespace>/change-stream/<node id>/manifest/<...>.json
//	<backup id>/ns/<namespace>/change-stream/<node id>/data/<...>.seg
//
// metadata.json records no counts. The query stream has one manifest per
// partition that holds records, and none for an empty partition. Each counts the
// records and bytes of the two rounds the server scans its partition in:
//   - num_base_* for the base round, which reads every record of the partition
//     once;
//   - num_incr_* for the incremental round, a second full scan that keeps the
//     records updated at or after the backup start less the cluster clock skew,
//     a fixed 20 seconds for an AP namespace.
//
// The change stream has one manifest per node, whose num_records and num_bytes
// count every write the node took while the backup ran, across all partitions.
// Every manifest lists its segments.
//
// ByteCount and FileCount are exact: they cover the segments of both rounds and
// of the change stream, and the byte counts of a manifest add up to the sizes of
// the segments it lists.
//
// RecordCount is an estimate: it counts the base round only. The server does not
// deduplicate the rounds and the change stream against each other, restore does,
// so summing them counts a record once per round and once per write that caught
// it. Any record updated within the clock skew before the backup started is in
// both rounds, so the sum doubles the count of a backup taken right after its
// data was loaded. The base round holds each record once, which makes it exact
// for a backup no write overlaps, and close otherwise:
//   - it misses the records inserted after the base round scanned their
//     partition;
//   - it still counts the records deleted after the base round scanned them;
//   - the server counts a base round that resumed after a failure slightly high.
//
// IncrRecordCount and ChangeRecordCount report the other two sources as they
// are. RecordCount + IncrRecordCount + ChangeRecordCount is an upper bound on the
// distinct records of the backup, and the change stream count tells how many
// writes the backup overlapped.
//
// Counting distinct records exactly needs reading every segment, which does not
// scale to large backups.
//
// A backup that does not exist has no manifests, and its statistics are zero.
func (l *Lister) GetStatistics(ctx context.Context, backupKey string) (models.Statistics, error) {
	manifests, err := l.listManifests(ctx, backupKey)
	if err != nil {
		return models.Statistics{}, fmt.Errorf("failed to list manifests of %s: %w", backupKey, err)
	}

	perManifest := make([]models.Statistics, len(manifests))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.concurrency)

	for i, key := range manifests {
		g.Go(func() error {
			stats, err := l.manifestStatistics(gctx, key)
			perManifest[i] = stats

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return models.Statistics{}, err
	}

	var total models.Statistics
	for _, stats := range perManifest {
		total.Add(stats)
	}

	return total, nil
}

// listManifests returns the keys of the manifests of both streams of every
// namespace of the backup at backupKey.
func (l *Lister) listManifests(ctx context.Context, backupKey string) ([]string, error) {
	namespaces, _, err := l.listDir(ctx, objectKey(backupKey, namespacesDir))
	if err != nil {
		return nil, err
	}

	var manifests []string

	for _, namespace := range namespaces {
		nodes, _, err := l.listDir(ctx, objectKey(namespace, changeStreamDir))
		if err != nil {
			return nil, err
		}

		manifestDirs := []string{objectKey(namespace, queryStreamDir, manifestDir)}
		for _, node := range nodes {
			manifestDirs = append(manifestDirs, objectKey(node, manifestDir))
		}

		for _, dir := range manifestDirs {
			_, files, err := l.listDir(ctx, dir)
			if err != nil {
				return nil, err
			}

			manifests = append(manifests, files...)
		}
	}

	return slices.DeleteFunc(manifests, func(key string) bool {
		return !strings.HasSuffix(key, manifestSuffix)
	}), nil
}

// manifestStatistics reads the counts of the manifest at key.
func (l *Lister) manifestStatistics(ctx context.Context, key string) (models.Statistics, error) {
	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return models.Statistics{}, fmt.Errorf("failed to get %s: %w", key, err)
	}

	defer out.Body.Close()

	var manifest streamManifest
	if err := json.NewDecoder(io.LimitReader(out.Body, maxManifestSize)).Decode(&manifest); err != nil {
		return models.Statistics{}, fmt.Errorf("failed to parse manifest %s: %w", key, err)
	}

	return manifest.statistics(), nil
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
