package latency_test

import (
	"math"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/latency"
)

const tolerance = 1e-9

func bucketEnd() time.Time {
	return time.Date(2026, time.June, 29, 12, 5, 0, 0, time.UTC)
}

func closeTo(got, want float64) bool {
	return math.Abs(got-want) <= tolerance*math.Max(1, math.Abs(want))
}

func TestPercentilesFoldSamplesSharingABucket(t *testing.T) {
	t.Parallel()

	buckets := latency.Percentiles([]latency.Sample{
		{BucketEnd: bucketEnd(), Bins: []int16{0, 100}, Weights: []int32{45, 5}},
		{BucketEnd: bucketEnd(), Bins: []int16{0, 200}, Weights: []int32{45, 5}},
	})

	if len(buckets) != 1 {
		t.Fatalf("got %d buckets, want the two samples folded into 1", len(buckets))
	}

	got := buckets[0]
	if !got.End.Equal(bucketEnd()) {
		t.Errorf("End = %s, want %s", got.End, bucketEnd())
	}

	for _, c := range []struct {
		name string
		got  float64
		want float64
	}{
		{"P90", got.P90, 1.004987562112089},
		{"P95", got.P95, 2.718304256397406},
		{"P99", got.P99, 7.352506945279107},
	} {
		if !closeTo(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestPercentilesRepeatABinWhenItCoversEveryTarget(t *testing.T) {
	t.Parallel()

	buckets := latency.Percentiles([]latency.Sample{
		{BucketEnd: bucketEnd(), Bins: []int16{-50}, Weights: []int32{7}},
	})

	if len(buckets) != 1 {
		t.Fatalf("got %d buckets, want 1", len(buckets))
	}

	got := buckets[0]
	if !closeTo(got.P90, 0.6110714560936471) || got.P90 != got.P95 || got.P95 != got.P99 {
		t.Errorf("single-bin bucket = %+v, want every quantile at 0.6110714560936471", got)
	}
}

func TestPercentilesReturnBucketsInTimeOrder(t *testing.T) {
	t.Parallel()

	second := bucketEnd().Add(5 * time.Minute)
	third := bucketEnd().Add(10 * time.Minute)

	buckets := latency.Percentiles([]latency.Sample{
		{BucketEnd: third, Bins: []int16{10}, Weights: []int32{1}},
		{BucketEnd: bucketEnd(), Bins: []int16{10}, Weights: []int32{1}},
		{BucketEnd: second, Bins: []int16{10}, Weights: []int32{1}},
	})

	want := []time.Time{bucketEnd(), second, third}
	if len(buckets) != len(want) {
		t.Fatalf("got %d buckets, want %d", len(buckets), len(want))
	}

	for i, end := range want {
		if !buckets[i].End.Equal(end) {
			t.Errorf("bucket %d ends at %s, want %s", i, buckets[i].End, end)
		}
	}
}

func TestPercentilesIgnoreBinsWithoutAWeight(t *testing.T) {
	t.Parallel()

	short := latency.Percentiles([]latency.Sample{
		{BucketEnd: bucketEnd(), Bins: []int16{0, 100, 200}, Weights: []int32{90}},
	})
	full := latency.Percentiles([]latency.Sample{
		{BucketEnd: bucketEnd(), Bins: []int16{0}, Weights: []int32{90}},
	})

	if len(short) != 1 || len(full) != 1 {
		t.Fatalf("got %d and %d buckets, want 1 each", len(short), len(full))
	}

	if short[0] != full[0] {
		t.Errorf("unweighted bins changed the result: %+v, want %+v", short[0], full[0])
	}
}

func TestPercentilesOfNothing(t *testing.T) {
	t.Parallel()

	if got := latency.Percentiles(nil); len(got) != 0 {
		t.Errorf("Percentiles(nil) = %+v, want no buckets", got)
	}

	zeroed := latency.Percentiles([]latency.Sample{
		{BucketEnd: bucketEnd(), Bins: []int16{5}, Weights: []int32{0}},
	})

	if len(zeroed) != 1 {
		t.Fatalf("got %d buckets, want 1", len(zeroed))
	}

	if want := (latency.Bucket{End: bucketEnd()}); zeroed[0] != want {
		t.Errorf("all-zero weights = %+v, want %+v", zeroed[0], want)
	}
}
