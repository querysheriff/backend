package humanize

import (
	"fmt"
	"strconv"
	"time"
)

const thousand = 1000.0

// Duration formats duration using its largest unit. Example: 90*time.Minute -> "1 hour".
func Duration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return plural(int(d.Seconds()), "second", "seconds")
	case d < time.Hour:
		return plural(int(d.Minutes()), "minute", "minutes")
	default:
		return plural(int(d.Hours()), "hour", "hours")
	}
}

// Millis formats milliseconds as ms or seconds. Example: 1500 -> "1.5 s".
func Millis(ms float64) string {
	if ms < thousand {
		return fmt.Sprintf("%.0f ms", ms)
	}

	return fmt.Sprintf("%.1f s", ms/thousand)
}

func plural(n int, singular, many string) string {
	if n == 1 {
		return "1 " + singular
	}

	return strconv.Itoa(n) + " " + many
}
