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

func TestSumRestoreStats(t *testing.T) {
	t.Parallel()

	t.Run("no args returns empty stats", func(t *testing.T) {
		t.Parallel()
		result := SumRestoreStats()
		require.NotNil(t, result)
		require.Equal(t, uint64(0), result.GetRecordsExpired())
		require.Equal(t, uint64(0), result.GetRecordsSkipped())
		require.Equal(t, uint64(0), result.GetRecordsIgnored())
		require.Equal(t, uint64(0), result.GetTotalBytesRead())
		require.Equal(t, uint64(0), result.GetRecordsExisted())
		require.Equal(t, uint64(0), result.GetRecordsFresher())
		require.Equal(t, uint64(0), result.GetRecordsInserted())
		require.Equal(t, uint64(0), result.GetErrorsInDoubt())
		require.Equal(t, uint64(0), result.GetRetryPolicyAttempts())
	})

	t.Run("nil stats are skipped", func(t *testing.T) {
		t.Parallel()
		result := SumRestoreStats(nil, nil)
		require.NotNil(t, result)
		require.Equal(t, uint64(0), result.GetRecordsExpired())
		require.Equal(t, uint64(0), result.GetRecordsInserted())
	})

	t.Run("single stat returns same values", func(t *testing.T) {
		t.Parallel()
		s := NewRestoreStats()
		s.RecordsExpired.Add(1)
		s.RecordsSkipped.Add(2)
		s.RecordsIgnored.Add(3)
		s.TotalBytesRead.Add(100)
		s.recordsExisted.Add(4)
		s.recordsFresher.Add(5)
		s.recordsInserted.Add(10)
		s.errorsInDoubt.Add(1)
		s.retryPolicyAttempts.Add(2)
		s.commonStats.ReadRecords.Add(10)
		s.AddUDFs(1)

		result := SumRestoreStats(s)
		require.NotNil(t, result)
		require.Equal(t, uint64(1), result.GetRecordsExpired())
		require.Equal(t, uint64(2), result.GetRecordsSkipped())
		require.Equal(t, uint64(3), result.GetRecordsIgnored())
		require.Equal(t, uint64(100), result.GetTotalBytesRead())
		require.Equal(t, uint64(4), result.GetRecordsExisted())
		require.Equal(t, uint64(5), result.GetRecordsFresher())
		require.Equal(t, uint64(10), result.GetRecordsInserted())
		require.Equal(t, uint64(1), result.GetErrorsInDoubt())
		require.Equal(t, uint64(2), result.GetRetryPolicyAttempts())
		require.Equal(t, uint64(10), result.GetReadRecords())
		require.Equal(t, uint32(1), result.GetUDFs())
	})

	t.Run("multiple stats are summed", func(t *testing.T) {
		t.Parallel()
		s1 := NewRestoreStats()
		s1.RecordsExpired.Add(1)
		s1.RecordsSkipped.Add(10)
		s1.recordsInserted.Add(100)
		s1.TotalBytesRead.Add(500)
		s1.commonStats.ReadRecords.Add(100)

		s2 := NewRestoreStats()
		s2.RecordsExpired.Add(2)
		s2.RecordsIgnored.Add(5)
		s2.recordsInserted.Add(200)
		s2.recordsFresher.Add(3)
		s2.errorsInDoubt.Add(1)
		s2.TotalBytesRead.Add(300)
		s2.commonStats.ReadRecords.Add(50)
		s2.AddSIndexes(2)

		result := SumRestoreStats(s1, s2)
		require.NotNil(t, result)
		require.Equal(t, uint64(3), result.GetRecordsExpired())
		require.Equal(t, uint64(10), result.GetRecordsSkipped())
		require.Equal(t, uint64(5), result.GetRecordsIgnored())
		require.Equal(t, uint64(800), result.GetTotalBytesRead())
		require.Equal(t, uint64(3), result.GetRecordsFresher())
		require.Equal(t, uint64(300), result.GetRecordsInserted())
		require.Equal(t, uint64(1), result.GetErrorsInDoubt())
		require.Equal(t, uint64(150), result.GetReadRecords())
		require.Equal(t, uint32(2), result.GetSIndexes())
	})

	t.Run("nil entries in slice are skipped", func(t *testing.T) {
		t.Parallel()
		s := NewRestoreStats()
		s.recordsInserted.Add(42)
		s.RecordsExpired.Add(1)

		result := SumRestoreStats(nil, s, nil)
		require.NotNil(t, result)
		require.Equal(t, uint64(42), result.GetRecordsInserted())
		require.Equal(t, uint64(1), result.GetRecordsExpired())
	})
}
