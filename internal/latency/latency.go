package latency

import (
	"math"
	"slices"
	"time"
)

const (
	binBase       = 1.01
	binMidpoint   = 0.5
	quantileCount = 3
)

type Sample struct {
	// BucketEnd identifies the time bucket end.
	// Example: 12:01 with 1m buckets means [12:00, 12:01).
	BucketEnd time.Time

	// Bins are logarithmic latency bucket IDs.
	// Example: 100=2.72, 200=7.35, 300=19.9 (where 2.72 is the geometric midpoint of logarithmic bin 100).
	Bins []int16

	// Weights are counts for matching Bins.
	// Example: Bins=[100,200], Weights=[90,10] means 90 values near 2.72, 10 near 7.35.
	Weights []int32
}

type Bucket struct {
	End time.Time
	P90 float64
	P95 float64
	P99 float64
}

// Percentiles groups samples by BucketEnd, merges their histogram bins, and returns P90/P95/P99 per time bucket.
//
// Example input:
//
//	[
//	  {BucketEnd: 12:01, Bins: [100, 200], Weights: [90, 10]},
//	  {BucketEnd: 12:01, Bins: [100],      Weights: [10]},
//	]
//
// Merged histogram for 12:01:
//
//	bin 100 -> 100 samples
//	bin 200 -> 10 samples
//
// Output:
//
//	[{End: 12:01, P90: 2.72, P95: 7.35, P99: 7.35}]
func Percentiles(samples []Sample) []Bucket {
	weights := map[time.Time]map[int16]int64{}

	for _, sample := range samples {
		bucket, ok := weights[sample.BucketEnd]
		if !ok {
			bucket = map[int16]int64{}
			weights[sample.BucketEnd] = bucket
		}

		for i, bin := range sample.Bins {
			if i >= len(sample.Weights) {
				break
			}

			bucket[bin] += int64(sample.Weights[i])
		}
	}

	ends := make([]time.Time, 0, len(weights))
	for end := range weights {
		ends = append(ends, end)
	}

	slices.SortFunc(ends, time.Time.Compare)

	buckets := make([]Bucket, len(ends))

	for i, end := range ends {
		q := quantiles(weights[end])
		buckets[i] = Bucket{End: end, P90: q[0], P95: q[1], P99: q[2]}
	}

	return buckets
}

func quantiles(weightByBin map[int16]int64) [quantileCount]float64 {
	var (
		found [quantileCount]float64
		seen  [quantileCount]bool
		total int64
	)

	bins := make([]int16, 0, len(weightByBin))

	for bin, weight := range weightByBin {
		bins = append(bins, bin)
		total += weight
	}

	if total <= 0 {
		return found
	}

	slices.Sort(bins)

	logBase := math.Log(binBase)
	targets := [quantileCount]float64{0.90, 0.95, 0.99}

	var cumulative int64

	for _, bin := range bins {
		cumulative += weightByBin[bin]

		for t, target := range targets {
			if !seen[t] && float64(cumulative) >= target*float64(total) {
				seen[t] = true
				found[t] = math.Exp((float64(bin) + binMidpoint) * logBase)
			}
		}
	}

	return found
}
