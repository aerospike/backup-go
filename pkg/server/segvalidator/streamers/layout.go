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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// A server side backup has a fixed layout, and everything in this package is
// built on it. A namespace holds one directory per stream, and a stream holds
// its manifests together in one directory and its segments in another:
//
//	<backup id>/ns/<namespace>/query-stream/manifest/<partition>-<...>.json
//	<backup id>/ns/<namespace>/query-stream/data/p935/<...>.seg
//	<backup id>/ns/<namespace>/change-stream/<node id>/manifest/<...>.json
//	<backup id>/ns/<namespace>/change-stream/<node id>/data/<...>.seg
//
// The two streams differ in how deep the pair of directories sits and in
// whether the segments are grouped by partition, so neither is assumed: a
// stream is searched for the directories, and a data directory is used as it is
// found, with or without partitions below it.
//
// Knowing the layout is what makes a backup samplable. The manifests of a
// stream sit together in a directory of their own, so a handful of them can be
// picked without walking the segments they describe, and one of them names a
// whole partition worth of segments for the price of a single download.
const (
	// namespacesDir holds one directory per namespace of the backup.
	namespacesDir = "ns"
	// dataDir holds the segments of a stream, grouped by partition or not.
	dataDir = "data"
	// manifestDir holds every manifest of a stream.
	manifestDir = "manifest"
	// segmentSuffix ends the name of a segment file.
	segmentSuffix = ".seg"
	// manifestSuffix ends the name of a manifest file.
	manifestSuffix = ".json"
)

// maxManifestSize caps a manifest read, so a bogus file cannot keep a validator
// downloading forever. A manifest is walked as it arrives, so the limit bounds
// the transfer and not the memory reading one costs.
const maxManifestSize = 64 << 20

// Stream is one of the two streams a server side backup writes. They hold the
// same kind of data and are validated the same way; which one a segment came
// from is worth knowing only when reporting on it.
type Stream string

const (
	// QueryStream holds what the scan of the namespace wrote.
	QueryStream Stream = "query-stream"
	// ChangeStream holds the changes recorded while the backup was running.
	ChangeStream Stream = "change-stream"
)

// streams are the streams of a namespace, in the order they are visited.
var streams = []Stream{QueryStream, ChangeStream}

// ErrManifestUnusable is returned for a manifest that cannot be turned into a
// list of segments, whether because it did not parse or because it names
// something that cannot be located in the storage.
var ErrManifestUnusable = errors.New("manifest cannot be read")

// namespacesRoot locates the directory naming the namespaces of the backup.
func namespacesRoot(backupID string) string {
	return path.Join(backupID, namespacesDir)
}

// streamRoot locates the directory of one stream of one namespace. What sits
// below it is discovered rather than assumed.
func streamRoot(backupID, namespace string, stream Stream) string {
	return path.Join(backupID, namespacesDir, namespace, string(stream))
}

// isSegment reports whether a listed file is a segment. A data directory is
// expected to hold nothing else, but a stray file must not reach a validator as
// if it were backup data.
func isSegment(storagePath string) bool {
	return strings.HasSuffix(storagePath, segmentSuffix)
}

// isManifest reports whether a listed file is a manifest.
func isManifest(storagePath string) bool {
	return strings.HasSuffix(storagePath, manifestSuffix)
}

// crc32Algorithm is the checksum a manifest records for its segments, and the
// only one this package knows how to check. A manifest recording another one
// has its checksums ignored rather than misread.
const crc32Algorithm = "crc32"

// manifestHeader is what a manifest says about itself. Every field is optional:
// the namespace of a manifest is also the directory it was found in, the
// partition only matters for a manifest that names its segments by their bare
// file name, and a manifest that does not say how it checksummed its segments
// is taken not to have.
type manifestHeader struct {
	Namespace string
	Partition string
	Algorithm string
}

// manifestSegment is one segment a manifest records: where it is, how big it
// was written and what it checksummed to. A manifest says more about a segment
// than this, but the rest describes its contents, which a validator learns by
// reading the segment itself.
type manifestSegment struct {
	SegmentName string `json:"segment_name"`
	Checksum    string `json:"checksum"`
	Size        int64  `json:"size"`
}

// decodeManifest walks a manifest, filling header in as it goes and handing
// every segment it records to fn.
//
// The segments are read one at a time instead of being unmarshalled into a
// slice, because a manifest of a busy partition records a lot of them while a
// caller keeps no more than the few it wants. A caller sending them on as they
// arrive sees of header only what the manifest wrote before them, which is why
// it is filled in in place rather than returned: a manifest states what it is
// before stating what it holds.
func decodeManifest(r io.Reader, header *manifestHeader, fn func(manifestSegment) error) error {
	dec := json.NewDecoder(io.LimitReader(r, maxManifestSize))

	if err := expectDelim(dec, '{'); err != nil {
		return err
	}

	for dec.More() {
		key, err := readKey(dec)
		if err != nil {
			return err
		}

		switch key {
		case "namespace":
			err = dec.Decode(&header.Namespace)
		case "partition_id":
			err = decodePartition(dec, &header.Partition)
		case "checksum_algorithm":
			err = dec.Decode(&header.Algorithm)
		case "segments":
			err = decodeSegments(dec, fn)
		default:
			// A manifest describes more than the segments it holds, and the
			// rest of it changes without this package caring.
			var skipped json.RawMessage
			err = dec.Decode(&skipped)
		}

		if err != nil {
			return fmt.Errorf("%w: field %q: %w", ErrManifestUnusable, key, err)
		}
	}

	return nil
}

// decodeSegments walks the segment array of a manifest.
func decodeSegments(dec *json.Decoder, fn func(manifestSegment) error) error {
	if err := expectDelim(dec, '['); err != nil {
		return err
	}

	for dec.More() {
		var seg manifestSegment

		if err := dec.Decode(&seg); err != nil {
			return err
		}

		if err := fn(seg); err != nil {
			return err
		}
	}

	// The closing bracket has to be consumed, so that whatever follows the
	// array is read as a field of the manifest and not as another segment.
	return expectDelim(dec, ']')
}

// decodePartition reads the partition a manifest belongs to, which it writes as
// a number, into the name of the directory holding that partition.
func decodePartition(dec *json.Decoder, out *string) error {
	var raw json.RawMessage

	if err := dec.Decode(&raw); err != nil {
		return err
	}

	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		*out = "p" + number.String()

		return nil
	}

	// A manifest that names its partition directly is taken at its word.
	return json.Unmarshal(raw, out)
}

// readKey reads the name of the next field of an object.
func readKey(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}

	key, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("%w: expected a field name, got %v", ErrManifestUnusable, tok)
	}

	return key, nil
}

// expectDelim consumes one delimiter, refusing a manifest shaped differently
// from what it must be.
func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	if got, ok := tok.(json.Delim); !ok || got != want {
		return fmt.Errorf("%w: expected %q, got %v", ErrManifestUnusable, want, tok)
	}

	return nil
}

// resolve turns the name a manifest records into the path of the segment in the
// storage.
//
// A manifest names a segment by its path from the root of the storage, the
// backup id included. A name without a slash is taken to be a file name in the
// data directory of the partition the manifest belongs to, so that a manifest
// written the short way is still usable.
func (m manifestSegment) resolve(data, partition string) (string, error) {
	name := strings.TrimSpace(m.SegmentName)

	switch {
	case name == "":
		return "", fmt.Errorf("%w: it records a segment without a name", ErrManifestUnusable)
	case strings.Contains(name, "/"):
		cleaned := path.Clean(name)
		if path.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return "", fmt.Errorf("%w: segment %q points outside the storage", ErrManifestUnusable, name)
		}

		return cleaned, nil
	default:
		return path.Join(data, partition, name), nil
	}
}
