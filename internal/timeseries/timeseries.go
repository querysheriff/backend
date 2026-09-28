package timeseries

import "time"

// maxPoints keeps charts to roughly this many buckets.
const maxPoints = 300

type Bounds struct {
	Bucket     time.Duration
	Anchor     time.Time
	RangeStart time.Time
}

// NewBounds creates aligned time-series buckets for a range.
// Example: 12:00-13:00 -> Bucket=1m, Anchor=13:00, RangeStart=11:59.
func NewBounds(from, to, now time.Time) Bounds {
	bucket := bucketFor(to.Sub(from))

	if to.After(now) {
		to = now
	}

	anchor := to.Truncate(time.Minute)

	return Bounds{
		Bucket:     bucket,
		Anchor:     anchor,
		RangeStart: binStart(from, anchor, bucket).Add(-bucket),
	}
}

// Ends returns all bucket end times from RangeStart to Anchor.
// Example: Bucket=1m, RangeStart=11:59, Anchor=12:02 -> [12:00, 12:01, 12:02].
func (b Bounds) Ends() []time.Time {
	ends := make([]time.Time, 0, maxPoints+1)
	for end := b.RangeStart.Add(b.Bucket); !end.After(b.Anchor); end = end.Add(b.Bucket) {
		ends = append(ends, end)
	}

	return ends
}

// bucketFor picks the smallest round bucket that keeps the range within maxPoints.
// Example: 24h -> 5m = 288 buckets. Very large ranges fall back to 1 week.
func bucketFor(d time.Duration) time.Duration {
	const day = 24 * time.Hour

	steps := [...]time.Duration{
		time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 30 * time.Minute,
		time.Hour, 2 * time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour, day, 7 * day,
	}
	for _, step := range steps {
		if d <= step*maxPoints {
			return step
		}
	}

	return steps[len(steps)-1]
}

func binStart(t, anchor time.Time, bucket time.Duration) time.Time {
	offset := t.Sub(anchor)

	bins := int64(offset / bucket)
	if offset%bucket != 0 && offset < 0 {
		bins--
	}

	return anchor.Add(time.Duration(bins) * bucket)
}
