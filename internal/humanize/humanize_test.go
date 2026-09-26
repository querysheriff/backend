package humanize_test

import (
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
