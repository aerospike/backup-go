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

// testASBPayload returns an uncompressed, unencrypted ASB file with n records.
// The bins repeat, so the payload compresses well.
func testASBPayload(t *testing.T, n int) []byte {
	t.Helper()

	encoder := NewEncoder("test", false, models.SIndexInfo{})

	var payload bytes.Buffer

	payload.Write(encoder.GetHeader(true))

	for i := range n {
		key, aerr := a.NewKey("test", "demo", i)
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

	var w io.WriteCloser = nopWriteCloser{&stored}

	if key != nil {
		encWriter, err := encryption.NewWriter(w, key)
		require.NoError(t, err)

		w = encWriter
	}

	encryptedWriter := w

	if compress {
		zstdWriter, err := zstd.NewWriter(w)
		require.NoError(t, err)

		w = zstdWriter
	}

	_, err := w.Write(plain)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	if compress {
		require.NoError(t, encryptedWriter.Close())
	}

	return stored.Bytes()
}

// TestFileReaderProcessor_StorageBytesRead checks that restore counts bytes as stored,
// so that restore progress is comparable with the size of the backup files. Token
// sizes, and so TotalBytesRead, count the decoded bytes instead.
func TestFileReaderProcessor_StorageBytesRead(t *testing.T) {
	t.Parallel()

	encryptionKey := bytes.Repeat([]byte{0x2a}, 32)

	tests := []struct {
		name     string
		compress bool
		key      []byte
	}{
		{name: "plain"},
		{name: "compressed", compress: true},
		{name: "encrypted", key: encryptionKey},
		{name: "compressed and encrypted", compress: true, key: encryptionKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plain := testASBPayload(t, 200)
			stored := storeASBPayload(t, plain, tt.compress, tt.key)

			config := NewDefaultRestoreConfig()
			if tt.compress {
				config.CompressionPolicy = NewCompressionPolicy(CompressZSTD, 0)
			}

			logger := slog.New(slog.DiscardHandler)
			kbpsCollector := metrics.NewCollector(t.Context(), logger, metrics.KilobytesPerSecond, "", false)

			var storageBytesRead atomic.Uint64

			fr := newFileReaderProcessor(nil, config, tt.key, kbpsCollector, &storageBytesRead, nil, nil, logger)

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

			require.Equal(t, 200, tokens)
			assert.Equal(t, uint64(len(stored)), storageBytesRead.Load(),
				"storage bytes must match the stored file size")

			if tt.compress {
				assert.Less(t, storageBytesRead.Load(), decodedSize,
					"a compressed file must read fewer bytes from storage than it decodes")
			}
		})
	}
}
