//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/testdb"
)

type lockScene struct {
	client *clickhouse.Client
	scope  clickhouse.TransactionScope
	base   time.Time
}

func newLockScene(t *testing.T, server string) lockScene {
	t.Helper()

	ctx := context.Background()
	client := clickhouse.New(testdb.ClickHouse(t))
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)

	blockerXact := base.Add(1 * time.Second)
	waiterXact := base.Add(20 * time.Second)
	waitStart := base.Add(28*time.Second + 784*time.Millisecond)

	blockerID := clickhouse.TransactionID(server, 20, base, blockerXact)
	waiterID := clickhouse.TransactionID(server, 10, base, waiterXact)

	observe := func(id uint64, pid uint32, xact, queryStart, at time.Time, query string) clickhouse.TransactionActivity {
		return clickhouse.TransactionActivity{
			TransactionID: id, ServerName: server, DatabaseName: "shop", UserName: "app",
			ApplicationName: "web", Pid: pid, BackendStart: base, XactStart: xact,
			CollectedAt: at, QueryStart: queryStart, Query: query, State: "active",
			QueryTags: map[string]string{},
		}
	}

	rows := []clickhouse.TransactionActivity{
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(17*time.Second),
			base.Add(18*time.Second),
			"SELECT before the lock",
		),
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(23*time.Second),
			base.Add(24*time.Second),
			"UPDATE that takes the lock",
		),
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(23*time.Second),
			base.Add(28*time.Second),
			"UPDATE that takes the lock",
		),
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(29*time.Second),
			base.Add(30*time.Second),
			"UPDATE after the block began",
		),
		observe(blockerID, 20, blockerXact, base.Add(40*time.Second), base.Add(41*time.Second), "SELECT later still"),
	}

	for _, at := range []int{29, 31, 33} {
		blocked := observe(waiterID, 10, waiterXact, base.Add(25*time.Second),
			base.Add(time.Duration(at)*time.Second), "UPDATE that waits")
		blocked.BlockedByPid = 20
		blocked.LockMode = "ExclusiveLock"
		blocked.WaitEventType = "Lock"
		blocked.LockWaitStart = &waitStart
		rows = append(rows, blocked)
	}

	if err := client.InsertTransactionActivity(ctx, rows); err != nil {
		t.Fatalf("seed lock scene: %v", err)
	}

	return lockScene{
		client: client,
		base:   base,
		scope: clickhouse.TransactionScope{
			ServerName: server, DatabaseName: "shop",
			From: base.Add(-time.Minute), To: base.Add(2 * time.Minute),
		},
	}
}

func TestLockWaitNamesTheQueryHoldingTheLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scene := newLockScene(t, "lock-holding-query")

	waits, err := scene.client.ListLockWaits(ctx, scene.scope, "waited", true, 10, 0)
	if err != nil {
		t.Fatalf("ListLockWaits: %v", err)
	}

	if len(waits) != 1 {
		t.Fatalf("got %d lock waits, want the single episode", len(waits))
	}

	wait := waits[0]

	if wait.BlockingQuery != "UPDATE that takes the lock" {
		t.Errorf("blocking query = %q, want the query running when the block began", wait.BlockingQuery)
	}

	if wait.WaitingQuery != "UPDATE that waits" {
		t.Errorf("waiting query = %q, want the blocked query", wait.WaitingQuery)
	}

	if wait.WaitingPid != 10 || wait.BlockedByPid != 20 {
		t.Errorf("pids = %d/%d, want 10 blocked by 20", wait.WaitingPid, wait.BlockedByPid)
	}

	if wait.WaitStart.Nanosecond() == 0 {
		t.Errorf("wait start %s lost its milliseconds", wait.WaitStart)
	}
}

func TestLockWaitBlockingQueryIsStableAsTheBlockerMovesOn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scene := newLockScene(t, "lock-stable-query")

	narrow := scene.scope
	narrow.To = scene.base.Add(35 * time.Second)

	early, err := scene.client.ListLockWaits(ctx, narrow, "waited", true, 10, 0)
	if err != nil {
		t.Fatalf("ListLockWaits (narrow): %v", err)
	}

	late, err := scene.client.ListLockWaits(ctx, scene.scope, "waited", true, 10, 0)
	if err != nil {
		t.Fatalf("ListLockWaits (wide): %v", err)
	}

	if len(early) != 1 || len(late) != 1 {
		t.Fatalf("got %d and %d episodes, want one each", len(early), len(late))
	}

	if early[0].BlockingQuery != late[0].BlockingQuery {
		t.Errorf("blocking query changed from %q to %q once the blocker ran more queries",
			early[0].BlockingQuery, late[0].BlockingQuery)
	}

	if early[0].WaitStart != late[0].WaitStart {
		t.Errorf("wait start moved from %s to %s", early[0].WaitStart, late[0].WaitStart)
	}
}

func TestLockWaitPicksTheEarliestQueryWhenNonePrecedesTheWait(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server := "lock-none-before"
	client := clickhouse.New(testdb.ClickHouse(t))
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)

	blockerXact := base.Add(1 * time.Second)
	waiterXact := base.Add(2 * time.Second)
	waitStart := base.Add(5*time.Second + 400*time.Millisecond)

	blockerID := clickhouse.TransactionID(server, 20, base, blockerXact)
	waiterID := clickhouse.TransactionID(server, 10, base, waiterXact)

	observe := func(id uint64, pid uint32, xact, queryStart, at time.Time, query string) clickhouse.TransactionActivity {
		return clickhouse.TransactionActivity{
			TransactionID: id, ServerName: server, DatabaseName: "shop", UserName: "app",
			ApplicationName: "web", Pid: pid, BackendStart: base, XactStart: xact,
			CollectedAt: at, QueryStart: queryStart, Query: query, State: "active",
			QueryTags: map[string]string{},
		}
	}

	rows := []clickhouse.TransactionActivity{
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(6*time.Second),
			base.Add(6*time.Second),
			"first query after the wait",
		),
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(9*time.Second),
			base.Add(9*time.Second),
			"second query after the wait",
		),
		observe(
			blockerID,
			20,
			blockerXact,
			base.Add(20*time.Second),
			base.Add(20*time.Second),
			"last query after the wait",
		),
	}

	for _, at := range []int{6, 8} {
		blocked := observe(waiterID, 10, waiterXact, base.Add(3*time.Second),
			base.Add(time.Duration(at)*time.Second), "UPDATE that waits")
		blocked.BlockedByPid = 20
		blocked.LockMode = "ExclusiveLock"
		blocked.LockWaitStart = &waitStart
		rows = append(rows, blocked)
	}

	if err := client.InsertTransactionActivity(ctx, rows); err != nil {
		t.Fatalf("seed: %v", err)
	}

	waits, err := client.ListLockWaits(ctx, clickhouse.TransactionScope{
		ServerName: server, DatabaseName: "shop",
		From: base.Add(-time.Minute), To: base.Add(2 * time.Minute),
	}, "waited", true, 10, 0)
	if err != nil {
		t.Fatalf("ListLockWaits: %v", err)
	}

	if len(waits) != 1 {
		t.Fatalf("got %d lock waits, want one", len(waits))
	}

	if waits[0].BlockingQuery != "first query after the wait" {
		t.Errorf("blocking query = %q, want the earliest query after the wait began", waits[0].BlockingQuery)
	}
}

func TestLockWaitSeriesCountsOneWaitOnceAcrossBlockerChanges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server := "lock-series-blocker-change"
	client := clickhouse.New(testdb.ClickHouse(t))
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)

	waitStart := base.Add(-30 * time.Second)
	waiterXact := base.Add(-time.Minute)
	waiterID := clickhouse.TransactionID(server, 10, base, waiterXact)

	var rows []clickhouse.TransactionActivity

	for _, sample := range []struct {
		at      time.Duration
		blocker uint32
	}{{2 * time.Second, 20}, {5 * time.Second, 20}, {8 * time.Second, 30}, {10 * time.Second, 30}} {
		rows = append(rows, clickhouse.TransactionActivity{
			TransactionID: waiterID, ServerName: server, DatabaseName: "shop", UserName: "app",
			ApplicationName: "web", Pid: 10, BackendStart: base, XactStart: waiterXact,
			CollectedAt: base.Add(sample.at), QueryStart: waiterXact, Query: "UPDATE that waits",
			State: "active", QueryTags: map[string]string{}, BlockedByPid: sample.blocker,
			LockMode: "ExclusiveLock", WaitEventType: "Lock", LockWaitStart: &waitStart,
		})
	}

	if err := client.InsertTransactionActivity(ctx, rows); err != nil {
		t.Fatalf("seed: %v", err)
	}

	bins, err := client.LockWaitSeries(ctx, clickhouse.TransactionScope{
		ServerName: server, DatabaseName: "shop", From: base, To: base.Add(2 * time.Minute),
	}, time.Minute)
	if err != nil {
		t.Fatalf("LockWaitSeries with a wait that began before the window: %v", err)
	}

	var total float64
	for _, bin := range bins {
		total += bin.WaitSeconds
	}

	if total != 10 {
		t.Errorf("total wait = %vs, want 10s: the window start to the last sample, counted once", total)
	}
}
