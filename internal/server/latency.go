package server

import (
	"math"
	"slices"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// latencyBinBase is the histogram's bin width: each bin covers 1% more latency than the one below it.
const latencyBinBase = 1.01

const binMidpointOffset = 0.5

// latencyMinutes is one minute's histogram.
type latencyMinute struct {
	start   time.Time
	bins    []int16
	weights []int32
}

// latencyPercentiles folds every minute into its display bucket.
func latencyPercentiles(
	bounds seriesBounds,
	minutes []latencyMinute,
) (*querysheriffv1.StatementMetric, *querysheriffv1.StatementMetric, *querysheriffv1.StatementMetric) {
	byBucket := make(map[time.Time]map[int16]int64)

	for _, minute := range minutes {
		bucketEnd := binStart(minute.start, bounds.anchor, bounds.bucket).Add(bounds.bucket)

		bucket, ok := byBucket[bucketEnd]
		if !ok {
			bucket = make(map[int16]int64)
			byBucket[bucketEnd] = bucket
		}

		for i, bin := range minute.bins {
			if i >= len(minute.weights) {
				break
			}

			bucket[bin] += int64(minute.weights[i])
		}
	}

	ends := make([]time.Time, 0, len(byBucket))
	for end := range byBucket {
		ends = append(ends, end)
	}
	sort.Slice(ends, func(i, j int) bool { return ends[i].Before(ends[j]) })

	points90 := make([]*querysheriffv1.MetricPoint, len(ends))
	points95 := make([]*querysheriffv1.MetricPoint, len(ends))
	points99 := make([]*querysheriffv1.MetricPoint, len(ends))

	for i, end := range ends {
		at := timestamppb.New(end)
		q90, q95, q99 := bucketQuantiles(byBucket[end])

		points90[i] = &querysheriffv1.MetricPoint{At: at, Value: q90}
		points95[i] = &querysheriffv1.MetricPoint{At: at, Value: q95}
		points99[i] = &querysheriffv1.MetricPoint{At: at, Value: q99}
	}

	return statementMetric(points90), statementMetric(points95), statementMetric(points99)
}

func bucketQuantiles(weightByBin map[int16]int64) (float64, float64, float64) {
	bins := make([]int16, 0, len(weightByBin))

	var total int64

	for bin, weight := range weightByBin {
		bins = append(bins, bin)
		total += weight
	}

	if total == 0 {
		return 0, 0, 0
	}

	slices.Sort(bins)

	logBase := math.Log(latencyBinBase)
	targets := [3]float64{0.90, 0.95, 0.99}
	found := [3]float64{}

	var cumulative int64

	for _, bin := range bins {
		cumulative += weightByBin[bin]

		for t, target := range targets {
			if found[t] == 0 && float64(cumulative) >= target*float64(total) {
				found[t] = math.Exp((float64(bin) + binMidpointOffset) * logBase)
			}
		}
	}

	return found[0], found[1], found[2]
}
