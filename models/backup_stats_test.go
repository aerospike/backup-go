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

package models

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSumBackupStats(t *testing.T) {
	t.Parallel()

	t.Run("no args returns empty stats", func(t *testing.T) {
		t.Parallel()
		result := SumBackupStats()
		require.NotNil(t, result)
		require.Equal(t, uint64(0), result.GetFileCount())
		require.Equal(t, uint64(0), result.TotalRecords.Load())
		require.Equal(t, uint64(0), result.GetReadRecords())
	})

	t.Run("nil stats are skipped", func(t *testing.T) {
		t.Parallel()
		result := SumBackupStats(nil, nil)
		require.NotNil(t, result)
		require.Equal(t, uint64(0), result.GetFileCount())
		require.Equal(t, uint64(0), result.TotalRecords.Load())
	})

	t.Run("single stat returns same values", func(t *testing.T) {
		t.Parallel()
		s := NewBackupStats()
		s.IncFiles()
		s.IncFiles()
		s.TotalRecords.Store(100)
		s.ReadRecords.Add(50)
		s.BytesWritten.Add(200)
		s.AddSIndexes(1)
		s.AddUDFs(2)

		result := SumBackupStats(s)
		require.NotNil(t, result)
		require.Equal(t, uint64(2), result.GetFileCount())
		require.Equal(t, uint64(100), result.TotalRecords.Load())
		require.Equal(t, uint64(50), result.GetReadRecords())
		require.Equal(t, uint64(200), result.GetBytesWritten())
		require.Equal(t, uint32(1), result.GetSIndexes())
		require.Equal(t, uint32(2), result.GetUDFs())
	})

	t.Run("multiple stats are summed", func(t *testing.T) {
		t.Parallel()
		s1 := NewBackupStats()
		s1.IncFiles()
		s1.TotalRecords.Store(10)
		s1.ReadRecords.Add(5)
		s1.AddUDFs(1)

		s2 := NewBackupStats()
		s2.IncFiles()
		s2.IncFiles()
		s2.TotalRecords.Store(20)
		s2.ReadRecords.Add(15)
		s2.BytesWritten.Add(100)
		s2.AddSIndexes(2)

		result := SumBackupStats(s1, s2)
		require.NotNil(t, result)
		require.Equal(t, uint64(3), result.GetFileCount())
		require.Equal(t, uint64(30), result.TotalRecords.Load())
		require.Equal(t, uint64(20), result.GetReadRecords())
		require.Equal(t, uint64(100), result.GetBytesWritten())
		require.Equal(t, uint32(2), result.GetSIndexes())
		require.Equal(t, uint32(1), result.GetUDFs())
	})

	t.Run("nil entries in slice are skipped", func(t *testing.T) {
		t.Parallel()
		s := NewBackupStats()
		s.IncFiles()
		s.TotalRecords.Store(7)

		result := SumBackupStats(nil, s, nil)
		require.NotNil(t, result)
		require.Equal(t, uint64(1), result.GetFileCount())
		require.Equal(t, uint64(7), result.TotalRecords.Load())
	})
}
