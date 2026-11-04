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

package backup

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/models"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPayloadNamespace = "test"
	testPayloadSet       = "demo"
	testPayloadRecords   = 200
)

// testASBPayload returns an uncompressed, unencrypted ASB file with n records.
// The bins repeat, so the payload compresses well.
func testASBPayload(t *testing.T, n int) []byte {
	t.Helper()

	encoder := NewEncoder(testPayloadNamespace, false, models.SIndexInfo{})

	var payload bytes.Buffer

	payload.Write(encoder.GetHeader(true))

	for i := range n {
		key, aerr := a.NewKey(testPayloadNamespace, testPayloadSet, i)
		require.NoError(t, aerr)

		token := models.NewRecordToken(&models.Record{
			Record: &a.Record{
				Key:        key,
				Bins:       a.BinMap{"b": strings.Repeat("x", 1000)},
				Generation: 1,
			},
		}, 0, nil)

		encoded, err := encoder.EncodeToken(token, nil)
		require.NoError(t, err)

		payload.Write(encoded)
	}

	return payload.Bytes()
}

// storeASBPayload compresses and then encrypts plain, the order a backup writes it in.
func storeASBPayload(t *testing.T, plain []byte, compress bool, key []byte) []byte {
	t.Helper()

	var stored bytes.Buffer

	// Each writer wraps the last one in the stack, the bottom one writes to stored.
	writers := []io.WriteCloser{nopWriteCloser{&stored}}

	if key != nil {
		encWriter, err := encryption.NewWriter(writers[len(writers)-1], key)
		require.NoError(t, err)

		writers = append(writers, encWriter)
	}

	if compress {
		zstdWriter, err := zstd.NewWriter(writers[len(writers)-1])
		require.NoError(t, err)

		writers = append(writers, zstdWriter)
	}

	_, err := writers[len(writers)-1].Write(plain)
	require.NoError(t, err)

	// Close from the top, so that each writer flushes into the one below it.
	for _, w := range slices.Backward(writers) {
		require.NoError(t, w.Close())
	}

	return stored.Bytes()
}

// TestFileReaderProcessor_StorageBytesRead checks that restore counts bytes as stored,
// so that restore progress is comparable with the size of the backup files. Token
// sizes, and so TotalBytesRead, count the decoded bytes instead.
func TestFileReaderProcessor_StorageBytesRead(t *testing.T) {
	t.Parallel()

	encryptionKey := bytes.Repeat([]byte{0x2a}, 32)
	compression := NewCompressionPolicy(CompressZSTD, 0)

	tests := []struct {
		name            string
		giveCompression *CompressionPolicy
		giveKey         []byte
		// wantStoredLess is whether fewer bytes are read from storage than are decoded.
		wantStoredLess bool
	}{
		{name: "plain"},
		{name: "compressed", giveCompression: compression, wantStoredLess: true},
		{name: "encrypted", giveKey: encryptionKey},
		{
			name:            "compressed and encrypted",
			giveCompression: compression,
			giveKey:         encryptionKey,
			wantStoredLess:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plain := testASBPayload(t, testPayloadRecords)
			stored := storeASBPayload(t, plain, tt.giveCompression != nil, tt.giveKey)

			config := NewDefaultRestoreConfig()
			config.CompressionPolicy = tt.giveCompression

			logger := slog.New(slog.DiscardHandler)
			kbpsCollector := metrics.NewCollector(t.Context(), logger, metrics.KilobytesPerSecond, "", false)

			var storageBytesRead atomic.Uint64

			fr := newFileReaderProcessor(nil, config, tt.giveKey, kbpsCollector, &storageBytesRead, nil, nil, logger)

			decoder, err := fr.initDecoder(io.NopCloser(bytes.NewReader(stored)), "test.asb")
			require.NoError(t, err)

			var (
				tokens      int
				decodedSize uint64
			)

			for {
				token, err := decoder.NextToken()
				if errors.Is(err, io.EOF) {
					break
				}

				require.NoError(t, err)

				tokens++
				decodedSize += token.GetSize()
			}

			require.Equal(t, testPayloadRecords, tokens)
			assert.Equal(t, uint64(len(stored)), storageBytesRead.Load(),
				"storage bytes must match the stored file size")
			assert.Equal(t, tt.wantStoredLess, storageBytesRead.Load() < decodedSize,
				"only a compressed file reads fewer bytes from storage than it decodes")
		})
	}
}
