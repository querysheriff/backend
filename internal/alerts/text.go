package alerts

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	maxQueryPreview = 80
	thousand        = 1000.0
	million         = 1_000_000.0
)

// QueryPreview collapses whitespace and truncates a statement to one Slack line.
func QueryPreview(query string) string {
	query = strings.Join(strings.Fields(query), " ")
	if len(query) > maxQueryPreview {
		return query[:maxQueryPreview] + "..."
	}

	return query
}

// HumanDuration writes a duration in its largest readable unit, not Go's 1m47.3s.
func HumanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return plural(int(d.Seconds()), "second", "seconds")
	case d < time.Hour:
		return plural(int(d.Minutes()), "minute", "minutes")
	default:
		return plural(int(d.Hours()), "hour", "hours")
	}
}

// formatMillis keeps the sub-second timings HumanDuration would round away.
func formatMillis(ms float64) string {
	if ms < thousand {
		return fmt.Sprintf("%.0f ms", ms)
	}

	return fmt.Sprintf("%.1f s", ms/thousand)
}

// formatCount shortens a large count to something read at a glance.
func formatCount(n int64) string {
	switch {
	case n >= million:
		return fmt.Sprintf("%.1fM", float64(n)/million)
	case n >= thousand:
		return fmt.Sprintf("%.1fk", float64(n)/thousand)
	default:
		return strconv.FormatInt(n, 10)
	}
}

func plural(n int, singular, many string) string {
	if n == 1 {
		return "1 " + singular
	}

	return strconv.Itoa(n) + " " + many
}
