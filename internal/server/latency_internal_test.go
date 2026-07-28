package server

import (
	"math"
	"testing"
	"time"
)

func binFor(ms float64) int16 {
	return int16(math.Floor(math.Log(ms) / math.Log(latencyBinBase)))
}

func TestLatencyPercentilesFoldsMinutesIntoBuckets(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	bounds := seriesBounds{bucket: 10 * time.Minute, anchor: anchor, rangeStart: anchor.Add(-30 * time.Minute)}

	slow, fast := binFor(100), binFor(10)

	// Two minutes inside one bucket: the weights for the same bin must add. 17 fast and 3
	// slow means the fast bin's cumulative 17 falls short of 0.90*20 = 18, so the slow bin
	// is the one that crosses.
	minutes := []latencyMinute{
		{start: anchor.Add(-10 * time.Minute), bins: []int16{fast, slow}, weights: []int32{9, 1}},
		{start: anchor.Add(-5 * time.Minute), bins: []int16{fast, slow}, weights: []int32{8, 2}},
		// A separate bucket, entirely fast.
		{start: anchor.Add(-20 * time.Minute), bins: []int16{fast}, weights: []int32{100}},
	}

	p90, p95, p99 := latencyPercentiles(bounds, minutes)

	if got := len(p90.GetSeries()); got != 2 {
		t.Fatalf("got %d buckets, want 2", got)
	}

	first, second := p90.GetSeries()[0], p90.GetSeries()[1]

	if !first.GetAt().AsTime().Equal(anchor.Add(-10 * time.Minute)) {
		t.Fatalf("first bucket ends at %s, want %s", first.GetAt().AsTime(), anchor.Add(-10*time.Minute))
	}

	// The all-fast bucket must report the fast bin, within the bin's own width.
	if !withinBinWidth(first.GetValue(), 10) {
		t.Errorf("all-fast bucket p90 = %v, want ~10ms", first.GetValue())
	}

	// 18 fast + 2 slow: 90% of 20 is 18, reached only once the slow bin is counted.
	if !withinBinWidth(second.GetValue(), 100) {
		t.Errorf("merged bucket p90 = %v, want ~100ms", second.GetValue())
	}

	if !withinBinWidth(p95.GetSeries()[1].GetValue(), 100) || !withinBinWidth(p99.GetSeries()[1].GetValue(), 100) {
		t.Errorf("p95/p99 = %v/%v, want ~100ms",
			p95.GetSeries()[1].GetValue(), p99.GetSeries()[1].GetValue())
	}
}

// withinBinWidth allows the ~0.5% the midpoint estimate can be off by, plus a margin for
// the flooring done when the bin was chosen.
func withinBinWidth(got, want float64) bool {
	return math.Abs(got-want)/want < 0.02
}

func TestLatencyPercentilesEmptyAndUnevenInput(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	bounds := seriesBounds{bucket: time.Minute, anchor: anchor, rangeStart: anchor.Add(-time.Hour)}

	p90, _, _ := latencyPercentiles(bounds, nil)
	if got := len(p90.GetSeries()); got != 0 {
		t.Fatalf("no input produced %d buckets, want 0", got)
	}

	// A weights array shorter than bins must not panic; the surplus bin is ignored.
	short := []latencyMinute{
		{start: anchor.Add(-time.Minute), bins: []int16{binFor(5), binFor(50)}, weights: []int32{7}},
	}

	p90, _, _ = latencyPercentiles(bounds, short)
	if got := len(p90.GetSeries()); got != 1 {
		t.Fatalf("got %d buckets, want 1", got)
	}

	if !withinBinWidth(p90.GetSeries()[0].GetValue(), 5) {
		t.Errorf("p90 = %v, want ~5ms from the only weighted bin", p90.GetSeries()[0].GetValue())
	}
}
