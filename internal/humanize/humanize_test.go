package humanize_test

import (
	"strings"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/humanize"
)

func TestDuration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0 seconds"},
		{time.Second, "1 second"},
		{90 * time.Second, "1 minute"},
		{2 * time.Minute, "2 minutes"},
		{time.Hour, "1 hour"},
		{25 * time.Hour, "25 hours"},
	}

	for _, c := range cases {
		if got := humanize.Duration(c.in); got != c.want {
			t.Errorf("Duration(%s) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestMillis(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   float64
		want string
	}{
		{0, "0 ms"},
		{12.4, "12 ms"},
		{999.9, "1000 ms"},
		{1000, "1.0 s"},
		{2500, "2.5 s"},
	}

	for _, c := range cases {
		if got := humanize.Millis(c.in); got != c.want {
			t.Errorf("Millis(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1.0k"},
		{12_500, "12.5k"},
		{999_999, "1000.0k"},
		{1_000_000, "1.0M"},
		{2_400_000, "2.4M"},
	}

	for _, c := range cases {
		if got := humanize.Count(c.in); got != c.want {
			t.Errorf("Count(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestQueryPreviewCollapsesWhitespace(t *testing.T) {
	t.Parallel()

	if got := humanize.QueryPreview("SELECT\n  1\n"); got != "SELECT 1" {
		t.Errorf("QueryPreview() = %q, want %q", got, "SELECT 1")
	}
}

func TestQueryPreviewTruncatesLongQueries(t *testing.T) {
	t.Parallel()

	got := humanize.QueryPreview(strings.Repeat("column, ", 40))
	if len(got) != humanize.MaxQueryPreview+len("...") {
		t.Errorf("QueryPreview() is %d long, want %d", len(got), humanize.MaxQueryPreview+len("..."))
	}

	if !strings.HasSuffix(got, "...") {
		t.Errorf("QueryPreview() = %q, want it to end in an ellipsis", got)
	}
}

func TestPlural(t *testing.T) {
	t.Parallel()

	cases := []struct {
		n    int
		want string
	}{
		{0, "0 queries"},
		{1, "1 query"},
		{2, "2 queries"},
	}

	for _, c := range cases {
		if got := humanize.Plural(c.n, "query", "queries"); got != c.want {
			t.Errorf("Plural(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}
