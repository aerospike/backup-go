package backup

import "math"

// distrStats holds the total, mean, and variance of the sample record sizes.
type distrStats struct {
	total    uint64
	mean     float64
	variance float64
}

// Function to calculate the estimate of total backup size
func estimateTotalBackupSize(
	headerSize, recordsSize uint64, recordsNumber, totalCount uint64, confidenceLevel float64, samples []uint64,
) uint64 {
	// Calculate record statistics (mean and variance)
	recStats := calcRecordStats(samples, recordsNumber)

	// Calculate z value (for upper-bound confidence interval)
	z := confidenceZ(confidenceLevel, totalCount)

	// Calculate compression ratio
	compressionRatio := float64(recordsSize) / float64(recStats.total+headerSize)

	// Estimate the total backup size
	var estBackupSize uint64
	if recordsNumber == 0 {
		estBackupSize = headerSize
	} else {
		estBackupSize = headerSize +
			uint64(math.Ceil(float64(totalCount)*(compressionRatio*recStats.mean+
				(z*math.Sqrt(recStats.variance/float64(recordsNumber))))))
	}

	return estBackupSize
}

// Function to calculate the z-value for confidence interval
func confidenceZ(confidenceLevel float64, totalCount uint64) float64 {
	q := (1 - confidenceLevel) / float64(totalCount)
	z := math.Sqrt(2) * -math.Erfinv(q*2-1)

	return z
}

// Calculate statistics (mean and variance) from the record size samples
func calcRecordStats(samples []uint64, nSamples uint64) distrStats {
	if nSamples <= 1 {
		return distrStats{0, 0, 0}
	}

	// Calculate total and mean
	var total uint64
	for _, sample := range samples {
		total += sample
	}

	mean := float64(total) / float64(nSamples)

	// Calculate variance
	var variance float64

	for _, sample := range samples {
		diff := float64(sample) - mean
		variance += diff * diff
	}

	variance /= float64(nSamples - 1)

	return distrStats{
		total:    total,
		mean:     mean,
		variance: variance,
	}
}
