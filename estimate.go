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

package backup

import (
	"log/slog"
	"math"

	"github.com/aerospike/backup-go/internal/util"
)

// zScore represents the number of standard deviations a given value is from the mean of a distribution.
// For a 99% confidence interval, the zScore is approximately 2.576.
// This means that there is a 99% probability that the true population parameter
// lies within the calculated confidence interval.
const zScore = 2.576

type estimateStats struct {
	Mean     float64
	Variance float64
}

func getEstimate(data []float64, total float64, logger *slog.Logger) float64 {
	stats := calculateStats(data)

	low, high := confidenceInterval(stats, len(data))
	logger.Debug("predicted record size:",
		slog.Float64("from", low),
		slog.Float64("to", high))

	avg := (low + high) / 2
	logger.Debug("average record size:", slog.Float64("average", avg))

	return total * avg
}

func calculateStats(data []float64) estimateStats {
	n := len(data)
	if n == 0 {
		return estimateStats{}
	}

	var sum float64
	for _, value := range data {
		sum += value
	}

	mean := sum / float64(n)

	// For n = 1, variance = 0
	if n == 1 {
		return estimateStats{
			mean,
			0.0,
		}
	}

	var varianceSum float64
	for _, value := range data {
		varianceSum += (value - mean) * (value - mean)
	}

	variance := varianceSum / float64(n-1)

	return estimateStats{
		Mean:     mean,
		Variance: variance,
	}
}

func confidenceInterval(stats estimateStats, sampleSize int) (low, high float64) {
	if sampleSize == 0 {
		return 0, 0
	}

	standardError := math.Sqrt(stats.Variance / float64(sampleSize))
	marginOfError := zScore * standardError

	low = stats.Mean - marginOfError
	high = stats.Mean + marginOfError

	return low, high
}

func getCompressRatio(policy *CompressionPolicy, samplesData []byte) (float64, error) {
	// We create io.WriteCloser from samplesData to calculate a compress ratio.
	bytesWriter := util.NewBytesWriteCloser([]byte{})
	// Create compression writer the same way as on backup.
	encodedWriter, err := newCompressionWriter(policy, bytesWriter)
	if err != nil {
		return 0, err
	}

	if _, err := encodedWriter.Write(samplesData); err != nil {
		return 0, err
	}

	if err := encodedWriter.Close(); err != nil {
		return 0, err
	}

	return float64(len(samplesData)) / float64(bytesWriter.Buffer().Len()), nil
}
