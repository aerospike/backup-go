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

package lazy

import (
	"context"
	"io"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testFileName = "test"
)

func TestLazyWriter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	filePath := path.Join(t.TempDir(), testFileName)

	openFunc := func(_ context.Context, _ int, _ *atomic.Uint64) (io.WriteCloser, error) {
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	writer, err := NewWriter(ctx, 1, openFunc)
	require.NoError(t, err)

	// Check that file not exist.
	_, err = os.Stat(filePath)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = writer.Write([]byte("Hello, World!"))
	require.NoError(t, err)

	// Now a file exists.
	_, err = os.Stat(filePath)
	require.NoError(t, err)
}
