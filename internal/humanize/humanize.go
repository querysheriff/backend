package humanize

import (
	"fmt"
	"strconv"
	"time"
)

const (
	thousand = 1000.0
	million  = 1_000_000.0
)

// Duration formats duration using its largest unit. Example: 90*time.Minute -> "1 hour".
func Duration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return Plural(int(d.Seconds()), "second", "seconds")
	case d < time.Hour:
		return Plural(int(d.Minutes()), "minute", "minutes")
	default:
		return Plural(int(d.Hours()), "hour", "hours")
	}
}

// Millis formats milliseconds as ms or seconds. Example: 1500 -> "1.5 s".
func Millis(ms float64) string {
	if ms < thousand {
		return fmt.Sprintf("%.0f ms", ms)
	}

	return fmt.Sprintf("%.1f s", ms/thousand)
}

// Count formats large counts compactly. Example: 1500 -> "1.5k".
func Count(n int64) string {
	switch {
	case n >= million:
		return fmt.Sprintf("%.1fM", float64(n)/million)
	case n >= thousand:
		return fmt.Sprintf("%.1fk", float64(n)/thousand)
	default:
		return strconv.FormatInt(n, 10)
	}
}

// Plural formats a count with singular or plural text. Example: Plural(2, "row", "rows") -> "2 rows".
func Plural(n int, singular, many string) string {
	if n == 1 {
		return "1 " + singular
	}

	return strconv.Itoa(n) + " " + many
}
