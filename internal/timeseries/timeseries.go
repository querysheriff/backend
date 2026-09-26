package timeseries

import "time"

const (
	points    = 60
	minBucket = time.Minute
)

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
	ends := make([]time.Time, 0, points+1)
	for end := b.RangeStart.Add(b.Bucket); !end.After(b.Anchor); end = end.Add(b.Bucket) {
		ends = append(ends, end)
	}

	return ends
}

func bucketFor(d time.Duration) time.Duration {
	bucket := d / points
	if bucket < minBucket {
		return minBucket
	}

	return bucket.Round(time.Minute)
}

func binStart(t, anchor time.Time, bucket time.Duration) time.Time {
	offset := t.Sub(anchor)

	bins := int64(offset / bucket)
	if offset%bucket != 0 && offset < 0 {
		bins--
	}

	return anchor.Add(time.Duration(bins) * bucket)
}
