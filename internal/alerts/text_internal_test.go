package alerts

import (
	"strings"
	"testing"
	"time"
)

func TestHumanDuration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   time.Duration
		want string
	}{
		{30 * time.Second, "30 seconds"},
		{time.Minute, "1 minute"},
		{59*time.Minute + 59*time.Second, "59 minutes"},
		{time.Hour + 43*time.Minute, "1 hour"},
		{50 * time.Hour, "50 hours"},
	}

	for _, c := range cases {
		if got := HumanDuration(c.in); got != c.want {
			t.Errorf("HumanDuration(%s) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFormatMillis(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   float64
		want string
	}{
		{0, "0 ms"},
		{340.4, "340 ms"},
		{999.99, "1000 ms"},
		{1400, "1.4 s"},
		{90200, "90.2 s"},
	}

	for _, c := range cases {
		if got := formatMillis(c.in); got != c.want {
			t.Errorf("formatMillis(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFormatCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1.0k"},
		{25768744, "25.8M"},
	}

	for _, c := range cases {
		if got := formatCount(c.in); got != c.want {
			t.Errorf("formatCount(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestQueryPreview(t *testing.T) {
	t.Parallel()

	if got := QueryPreview("SELECT\n  1\n"); got != "SELECT 1" {
		t.Errorf("whitespace not collapsed: %q", got)
	}

	long := QueryPreview(strings.Repeat("column, ", 40))
	if len(long) != maxQueryPreview+len("...") {
		t.Errorf("long query not truncated: %d chars", len(long))
	}
}
