//go:build integration

package clickhouse_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/logtaxonomy"
	"github.com/querysheriff/backend/internal/testdb"
)

func logEvent(serverName string, at time.Time, level, classification int32) clickhouse.LogEvent {
	return clickhouse.LogEvent{
		ID:         clickhouse.LogEventID(serverName, at, 0, strconv.FormatInt(at.UnixNano(), 10), 0),
		ServerName: serverName, CollectedAt: at, OccurredAt: at,
		LogLevel: level, Classification: classification,
	}
}

func TestLogHistogramBucketsEveryMatchingEvent(t *testing.T) {
	t.Parallel()

	const serverName = "log-histogram-test"

	ctx := context.Background()
	stats := clickhouse.New(testdb.ClickHouse(t))
	now := time.Now().UTC().Truncate(time.Minute)
	from := now.Add(-10 * time.Minute)

	delayed := logEvent(serverName, now, 5, 49)
	delayed.OccurredAt = from.Add(-2 * time.Hour)
	delayed.ID = clickhouse.LogEventID(serverName, delayed.OccurredAt, 1, "delayed", 0)

	events := []clickhouse.LogEvent{delayed}

	for i := 1; i <= 3; i++ {
		at := now.Add(-time.Duration(i) * time.Minute).Add(500 * time.Millisecond)
		event := logEvent(serverName, at, 5, 49)
		event.ID = clickhouse.LogEventID(serverName, at, uint32(i), "recent", int64(i))
		events = append(events, event)
	}

	if err := stats.InsertLogEvents(ctx, events); err != nil {
		t.Fatalf("seed: %v", err)
	}

	bins, err := stats.LogEventHistogram(ctx, clickhouse.LogFilter{
		ServerName: serverName,
		From:       from,
		To:         now,
	}, time.Minute)
	if err != nil {
		t.Fatalf("LogEventHistogram: %v", err)
	}

	var total int64
	for _, bin := range bins {
		total += bin.Count
	}

	if total != 3 {
		t.Errorf("histogram counted %d events across %d buckets, want the 3 inside the window",
			total, len(bins))
	}
}

func seedLogRow(serverName string, at time.Time, level, classification int32,
	message, database, user string, pid uint32) clickhouse.LogEvent {
	event := logEvent(serverName, at, level, classification)
	event.Message = message
	event.DatabaseName = database
	event.UserName = user
	event.Pid = pid
	event.ID = clickhouse.LogEventID(serverName, at, pid, message, 0)

	return event
}

func TestListLogEventsAppliesEveryFilter(t *testing.T) {
	t.Parallel()

	const serverName = "log-filter-test"

	ctx := context.Background()
	stats := clickhouse.New(testdb.ClickHouse(t))
	now := time.Now().UTC().Truncate(time.Minute)

	rows := []clickhouse.LogEvent{
		seedLogRow(serverName, now.Add(-5*time.Minute), 5, 49, "deadlock detected", "shop", "app", 11),
		seedLogRow(serverName, now.Add(-4*time.Minute), 6, 51, "duration: 900ms", "shop", "app", 12),
		seedLogRow(serverName, now.Add(-3*time.Minute), 7, 49, "fatal trouble", "billing", "batch", 13),
		seedLogRow(serverName, now.Add(-2*time.Minute), 6, 28, "checkpoint complete", "billing", "batch", 14),
	}

	other := seedLogRow("log-filter-other", now.Add(-time.Minute), 5, 49, "deadlock detected", "shop", "app", 15)
	if err := stats.InsertLogEvents(ctx, append(rows, other)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base := clickhouse.LogFilter{ServerName: serverName, From: now.Add(-time.Hour), To: now}
	order := clickhouse.LogSortOrder{Key: "at", Desc: true, SeverityOf: logtaxonomy.SeverityRanks()}

	cases := []struct {
		name   string
		filter func(clickhouse.LogFilter) clickhouse.LogFilter
		want   int
	}{
		{"unfiltered stays on this server", func(f clickhouse.LogFilter) clickhouse.LogFilter { return f }, 4},
		{"by level", func(f clickhouse.LogFilter) clickhouse.LogFilter { f.Levels = []int32{6}; return f }, 2},
		{
			"by classification",
			func(f clickhouse.LogFilter) clickhouse.LogFilter { f.Classifications = []int32{49}; return f },
			2,
		},
		{
			"by database",
			func(f clickhouse.LogFilter) clickhouse.LogFilter { f.Databases = []string{"billing"}; return f },
			2,
		},
		{"by user", func(f clickhouse.LogFilter) clickhouse.LogFilter { f.Usernames = []string{"app"}; return f }, 2},
		{
			"search matches message",
			func(f clickhouse.LogFilter) clickhouse.LogFilter { f.Search = "deadlock"; return f },
			1,
		},
		{"search matches pid", func(f clickhouse.LogFilter) clickhouse.LogFilter { f.Search = "13"; return f }, 1},
		{"window excludes older", func(f clickhouse.LogFilter) clickhouse.LogFilter {
			f.From = now.Add(-150 * time.Second)

			return f
		}, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got, err := stats.ListLogEvents(ctx, c.filter(base), order, 50, 0)
			if err != nil {
				t.Fatalf("ListLogEvents: %v", err)
			}

			if len(got) != c.want {
				t.Errorf("got %d events, want %d", len(got), c.want)
			}
		})
	}
}

func TestListLogEventsSortsAndPages(t *testing.T) {
	t.Parallel()

	const serverName = "log-sort-test"

	ctx := context.Background()
	stats := clickhouse.New(testdb.ClickHouse(t))
	now := time.Now().UTC().Truncate(time.Minute)

	rows := []clickhouse.LogEvent{
		seedLogRow(serverName, now.Add(-3*time.Minute), 6, 28, "oldest", "a", "u1", 21),
		seedLogRow(serverName, now.Add(-2*time.Minute), 5, 49, "middle", "b", "u2", 22),
		seedLogRow(serverName, now.Add(-1*time.Minute), 7, 51, "newest", "c", "u3", 23),
	}

	if err := stats.InsertLogEvents(ctx, rows); err != nil {
		t.Fatalf("seed: %v", err)
	}

	filter := clickhouse.LogFilter{ServerName: serverName, From: now.Add(-time.Hour), To: now}

	newestFirst, err := stats.ListLogEvents(ctx, filter,
		clickhouse.LogSortOrder{Key: "at", Desc: true, SeverityOf: logtaxonomy.SeverityRanks()}, 50, 0)
	if err != nil {
		t.Fatalf("ListLogEvents (at desc): %v", err)
	}

	if len(newestFirst) != 3 || newestFirst[0].Message != "newest" {
		t.Fatalf("sorted by time desc gave %q first, want newest", newestFirst[0].Message)
	}

	bySeverity, err := stats.ListLogEvents(ctx, filter,
		clickhouse.LogSortOrder{Key: "level", Desc: true, SeverityOf: logtaxonomy.SeverityRanks()}, 50, 0)
	if err != nil {
		t.Fatalf("ListLogEvents (level desc): %v", err)
	}

	want := []int32{7, 5, 6}
	for i, level := range want {
		if bySeverity[i].LogLevel != level {
			t.Errorf(
				"severity order[%d] = level %d, want %d (FATAL, ERROR, LOG - LOG is the least severe despite its higher enum value)",
				i,
				bySeverity[i].LogLevel,
				level,
			)
		}
	}

	page, err := stats.ListLogEvents(ctx, filter,
		clickhouse.LogSortOrder{Key: "at", Desc: true, SeverityOf: logtaxonomy.SeverityRanks()}, 1, 1)
	if err != nil {
		t.Fatalf("ListLogEvents (paged): %v", err)
	}

	if len(page) != 1 || page[0].Message != "middle" {
		t.Fatalf("offset 1 limit 1 gave %+v, want the middle row", page)
	}
}
