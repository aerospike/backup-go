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
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// The change stream of a node may hold more than one manifest. A node that had
// to restart its change stream starts a new manifest under a new regime and
// gives up what the old one recorded, because the scan that follows the restart
// captures it again. The server therefore restores a node from its newest
// manifest alone, the one with the highest regime and, within a regime, the
// latest timestamp, and never reads the others.
const (
	// changeStreamMarker precedes the node id in the path of a change stream
	// manifest.
	changeStreamMarker = "change-stream/"
	// changeManifestMarker follows the node id in the path of a change stream
	// manifest.
	changeManifestMarker = "/manifest/"
	// maxChangeNodeIDLen is the longest node id the server restores a change
	// stream of.
	maxChangeNodeIDLen = 63
	// maxChangeNodes is the number of nodes the server restores the change
	// stream of. A namespace whose change stream was written by more nodes
	// fails the restore.
	maxChangeNodes = 256
)

// changeCapture is when a change stream manifest was started: the regime of
// the cluster and the timestamp, in this order of precedence.
type changeCapture struct {
	timestamp uint64
	regime    uint32
}

// newer reports whether c is a more recent capture than other.
func (c changeCapture) newer(other changeCapture) bool {
	return c.regime > other.regime || (c.regime == other.regime && c.timestamp > other.timestamp)
}

// parseChangeManifest reads the node and the capture of a change stream
// manifest out of its path,
// .../change-stream/{node}/manifest/{regime}-{timestamp}-{uuid}.json, the way
// the server reads it on restore: the node is what sits between the markers,
// and sscanf(leaf, "%u-%llu-") must convert both numbers. ok is false for any
// other path, which the server skips on restore.
func parseChangeManifest(manifestPath string) (node string, capture changeCapture, ok bool) {
	_, rest, found := strings.Cut(manifestPath, changeStreamMarker)
	if !found {
		return "", changeCapture{}, false
	}

	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash > maxChangeNodeIDLen || !strings.HasPrefix(rest[slash:], changeManifestMarker) {
		return "", changeCapture{}, false
	}

	node = rest[:slash]
	leaf := rest[slash+len(changeManifestMarker):]

	sc := nameScanner{rest: leaf}

	regime, ok := sc.number(decimalBase)
	if !ok || !sc.literal(manifestNameSeparator) {
		return "", changeCapture{}, false
	}

	// The separator after the timestamp is no conversion, so sscanf counts
	// two whether or not it matches.
	timestamp, ok := sc.number(decimalBase)
	if !ok || !strings.HasSuffix(leaf, manifestSuffix) {
		return "", changeCapture{}, false
	}

	// %u stores into an unsigned int.
	return node, changeCapture{regime: uint32(regime), timestamp: timestamp}, true
}

// changeManifests lists the manifests of a change stream unit and returns the
// ones worth reading: the newest manifest of the node, which is the only one
// the server restores, and every manifest whose name the server cannot read,
// which are reported rather than silently passed over. The others are
// superseded and counted as such.
//
// A unit is the change stream of one node, so it has a single newest manifest.
// Of manifests started at the same capture the first one listed is kept, as
// the server keeps it.
//
// limit bounds the manifests the listing looks at, zero meaning all of them. A
// listing cut short may miss the newest manifest, which is then picked from the
// ones it looked at.
func (s *Streamer) changeManifests(ctx context.Context, u *unit, limit int) ([]file, error) {
	var (
		picked     []file
		newest     file
		best       changeCapture
		haveNewest bool
		capped     bool
		superseded int64
		scanned    int
	)

	err := s.store.listFiles(ctx, u.manifests(), func(f file) error {
		if !isManifest(f.Path) {
			return nil
		}

		if limit > 0 && scanned >= limit {
			capped = true

			return errStopListing
		}

		scanned++

		s.stats.manifestsFound.Add(1)

		_, capture, ok := parseChangeManifest(f.Path)

		switch {
		case !ok:
			picked = append(picked, f)
		case !haveNewest:
			newest, best, haveNewest = f, capture, true
		case capture.newer(best):
			newest, best = f, capture
			superseded++
		default:
			superseded++
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list manifests of %s: %w", u.manifests(), err)
	}

	s.stats.superseded.Add(superseded)

	if capped {
		s.logger.WarnContext(ctx, "the change stream manifests of the node were not all listed, "+
			"the newest of them may be missed",
			slog.String("namespace", u.namespace),
			slog.String("manifests", u.manifests()),
			slog.Int("limit", limit),
		)
	}

	if !haveNewest {
		return picked, nil
	}

	// Only a listing that saw every manifest of the node knows it is a node the
	// server restores. A node is counted once, by the unit that is the node.
	if !capped {
		s.countChangeNode(u)
	}

	return append(picked, newest), nil
}

// countChangeNode counts one node whose change stream a restore reads, and
// reports the namespace the first time it has more of them than the server
// restores, which fails the restore of its change stream.
func (s *Streamer) countChangeNode(u *unit) {
	if n := s.stats.addChangeNode(u.namespace); n == maxChangeNodes+1 {
		s.stats.addManifestIssue(u.namespace, streamRoot(s.backupID, u.namespace, ChangeStream),
			fmt.Errorf("%w: change stream of more than %d nodes, the server restores at most %d",
				ErrManifestUnusable, maxChangeNodes, maxChangeNodes))
	}
}

// listManifests calls fn for every manifest of a unit a restore reads, and
// stops without failing when fn returns errStopListing. limit bounds the
// manifests the listing looks at, zero meaning all of them.
//
// A query stream is restored from all of its manifests, which are handed over
// as they are listed. A change stream is restored from the newest manifest of
// its node, which is only known once all of them have been listed.
func (s *Streamer) listManifests(ctx context.Context, u *unit, limit int, fn func(file) error) error {
	if u.stream == ChangeStream {
		manifests, err := s.changeManifests(ctx, u, limit)
		if err != nil {
			return err
		}

		for _, m := range manifests {
			if err := fn(m); err != nil {
				return stopped(err)
			}
		}

		return nil
	}

	scanned := 0

	err := s.store.listFiles(ctx, u.manifests(), func(f file) error {
		if !isManifest(f.Path) {
			return nil
		}

		s.stats.manifestsFound.Add(1)

		if err := fn(f); err != nil {
			return err
		}

		scanned++
		if limit > 0 && scanned >= limit {
			return errStopListing
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("list manifests of %s: %w", u.manifests(), err)
	}

	return nil
}
