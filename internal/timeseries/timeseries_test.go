package timeseries_test

import (
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/timeseries"
)

func nowish() time.Time {
	return time.Date(2026, time.June, 29, 12, 0, 0, 0, time.UTC)
}

func TestBucketWidthScalesWithTheRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		span time.Duration
		want time.Duration
	}{
		{time.Minute, time.Minute},
		{time.Hour, time.Minute},
		{6 * time.Hour, 6 * time.Minute},
		{24 * time.Hour, 24 * time.Minute},
		{7 * 24 * time.Hour, 168 * time.Minute},
		{90 * time.Second, time.Minute},
	}

	for _, c := range cases {
		bounds := timeseries.NewBounds(nowish().Add(-c.span), nowish(), nowish())
		if bounds.Bucket != c.want {
			t.Errorf("span %s: bucket = %s, want %s", c.span, bounds.Bucket, c.want)
		}
	}
}

func TestBucketIsAWholeNumberOfMinutes(t *testing.T) {
	t.Parallel()

	for span := time.Minute; span <= 14*24*time.Hour; span += 37 * time.Second {
		bounds := timeseries.NewBounds(nowish().Add(-span), nowish(), nowish())
		if bounds.Bucket%time.Minute != 0 {
			t.Fatalf("span %s: bucket %s is not a whole number of minutes", span, bounds.Bucket)
		}

		if bounds.Bucket <= 0 {
			t.Fatalf("span %s: bucket %s is not positive", span, bounds.Bucket)
		}
	}
}

func TestAnchorClampsToNowAndTruncatesToTheMinute(t *testing.T) {
	t.Parallel()

	ragged := nowish().Add(42 * time.Second)

	bounds := timeseries.NewBounds(nowish().Add(-time.Hour), nowish().Add(time.Hour), ragged)
	if want := nowish(); !bounds.Anchor.Equal(want) {
		t.Errorf("future `to`: anchor = %s, want %s (nowish(), truncated)", bounds.Anchor, want)
	}

	bounds = timeseries.NewBounds(nowish().Add(-time.Hour), ragged, ragged)
	if want := nowish(); !bounds.Anchor.Equal(want) {
		t.Errorf("ragged `to`: anchor = %s, want %s", bounds.Anchor, want)
	}
}

func TestEndsAreContiguousAndEndAtTheAnchor(t *testing.T) {
	t.Parallel()

	for _, span := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour} {
		bounds := timeseries.NewBounds(nowish().Add(-span), nowish(), nowish())

		ends := bounds.Ends()
		if len(ends) == 0 {
			t.Fatalf("span %s: no bucket ends", span)
		}

		if want := bounds.RangeStart.Add(bounds.Bucket); !ends[0].Equal(want) {
			t.Errorf("span %s: first end = %s, want %s", span, ends[0], want)
		}

		if last := ends[len(ends)-1]; !last.Equal(bounds.Anchor) {
			t.Errorf("span %s: last end = %s, want the anchor %s", span, last, bounds.Anchor)
		}

		for i := 1; i < len(ends); i++ {
			if gap := ends[i].Sub(ends[i-1]); gap != bounds.Bucket {
				t.Fatalf("span %s: gap %s between ends %d and %d, want %s", span, gap, i-1, i, bounds.Bucket)
			}
		}
	}
}

func TestRangeStartPrecedesTheRequestedWindow(t *testing.T) {
	t.Parallel()

	for _, span := range []time.Duration{time.Hour, 6 * time.Hour, 7 * 24 * time.Hour} {
		from := nowish().Add(-span)

		bounds := timeseries.NewBounds(from, nowish(), nowish())
		if !bounds.RangeStart.Before(from) {
			t.Errorf("span %s: range start %s does not precede from %s", span, bounds.RangeStart, from)
		}

		if gap := from.Sub(bounds.RangeStart); gap > 2*bounds.Bucket {
			t.Errorf("span %s: range start %s is %s before from, want at most two buckets",
				span, bounds.RangeStart, gap)
		}
	}
}

func TestEveryInstantInRangeLandsOnABucketEnd(t *testing.T) {
	t.Parallel()

	bounds := timeseries.NewBounds(nowish().Add(-6*time.Hour), nowish(), nowish())

	ends := map[time.Time]bool{}
	for _, end := range bounds.Ends() {
		ends[end] = true
	}

	for at := bounds.RangeStart; at.Before(bounds.Anchor); at = at.Add(29 * time.Second) {
		end := bounds.BucketEnd(at)
		if !ends[end] {
			t.Fatalf("instant %s bucketed to %s, which is not one of the chart's ends", at, end)
		}

		if !end.After(at) || end.Sub(at) > bounds.Bucket {
			t.Fatalf("instant %s bucketed to %s, want a bucket end within %s after it", at, end, bounds.Bucket)
		}
	}
}

func TestBucketEndIsExclusiveOfItsOwnBoundary(t *testing.T) {
	t.Parallel()

	bounds := timeseries.NewBounds(nowish().Add(-6*time.Hour), nowish(), nowish())

	boundary := bounds.Anchor.Add(-bounds.Bucket)
	if got, want := bounds.BucketEnd(boundary), bounds.Anchor; !got.Equal(want) {
		t.Errorf("BucketEnd(%s) = %s, want %s", boundary, got, want)
	}

	if got, want := bounds.BucketEnd(boundary.Add(-time.Nanosecond)), boundary; !got.Equal(want) {
		t.Errorf("BucketEnd just before %s = %s, want %s", boundary, got, want)
	}
}
