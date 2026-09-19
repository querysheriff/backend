//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/testdb"
)

func txFixture(
	t *testing.T,
	txTestServer string,
) (*clickhouse.Client, clickhouse.TransactionScope, []clickhouse.TransactionActivity) {
	t.Helper()

	ctx := context.Background()
	client := clickhouse.New(testdb.ClickHouse(t))
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	waiter := clickhouse.TransactionID(txTestServer, 10, base, base.Add(5*time.Second))
	blocker := clickhouse.TransactionID(txTestServer, 20, base, base.Add(1*time.Second))
	lockAt := base.Add(9*time.Second + 250*time.Millisecond)

	row := func(id uint64, pid uint32, xact time.Time, at time.Time, state, query string) clickhouse.TransactionActivity {
		return clickhouse.TransactionActivity{
			TransactionID: id, ServerName: txTestServer, DatabaseName: "db",
			UserName: "app", ApplicationName: "web", Pid: pid,
			BackendStart: base, XactStart: xact, CollectedAt: at,
			QueryStart: xact, Query: query, QueryTags: map[string]string{"a": "b"},
			State: state,
		}
	}

	blocked := row(waiter, 10, base.Add(5*time.Second), lockAt, "active", "SELECT 1")
	blocked.WaitEventType = "Lock"
	blocked.WaitEvent = "tuple"
	blocked.BlockedByPid = 20
	blocked.LockWaitStart = &lockAt
	blocked.LockMode = "ExclusiveLock"

	rows := []clickhouse.TransactionActivity{
		row(waiter, 10, base.Add(5*time.Second), base.Add(6*time.Second), "active", "SELECT 1"),
		row(waiter, 10, base.Add(5*time.Second), base.Add(7*time.Second), "active", "SELECT 1"),
		row(waiter, 10, base.Add(5*time.Second), base.Add(8*time.Second), "idle in transaction", "SELECT 1"),
		blocked,
		row(blocker, 20, base.Add(1*time.Second), lockAt, "active", "UPDATE t SET x=1"),
		row(blocker, 20, base.Add(1*time.Second), lockAt.Add(2*time.Second), "active", "UPDATE t SET x=1"),
	}

	if err := client.InsertTransactionActivity(ctx, rows); err != nil {
		t.Fatalf("insert transaction activity: %v", err)
	}

	return client, clickhouse.TransactionScope{
		ServerName: txTestServer, DatabaseName: "db",
		From: base.Add(-time.Minute), To: base.Add(time.Minute),
	}, rows
}

func TestListTransactionsCollapsesObservations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, scope, _ := txFixture(t, "tx-db-test-list")

	txs, err := client.ListTransactions(ctx, scope, 0, "age", true, 50, 0)
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}

	if len(txs) != 2 {
		t.Fatalf("got %d transactions, want 2", len(txs))
	}

	if txs[0].Pid != 20 {
		t.Errorf("oldest-first by age gave pid %d, want 20", txs[0].Pid)
	}
}

func TestListTransactionEventsRebuildsRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, scope, _ := txFixture(t, "tx-db-test-runs")

	txs, err := client.ListTransactions(ctx, scope, 0, "age", true, 50, 0)
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}

	ids := make([]uint64, len(txs))
	for i, tx := range txs {
		ids[i] = tx.ID
	}

	events, err := client.ListTransactionEvents(ctx, scope, ids)
	if err != nil {
		t.Fatalf("ListTransactionEvents: %v", err)
	}

	var waiterRuns []clickhouse.TransactionEvent

	for _, event := range events {
		if event.Query == "SELECT 1" {
			waiterRuns = append(waiterRuns, event)
		}
	}

	if len(waiterRuns) != 3 {
		t.Fatalf("got %d runs for the waiting transaction, want 3 (active, idle, active+lock)", len(waiterRuns))
	}

	if got := waiterRuns[0].LastSeenAt.Sub(waiterRuns[0].FirstSeenAt); got != time.Second {
		t.Errorf("first run spans %s, want 1s (two observations collapsed)", got)
	}

	if waiterRuns[1].State != "idle in transaction" {
		t.Errorf("second run state = %q, want idle in transaction", waiterRuns[1].State)
	}
}

func TestListLockWaitsResolvesBlockingSide(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, scope, _ := txFixture(t, "tx-db-test-locks")

	waits, err := client.ListLockWaits(ctx, scope, "age", true, 50, 0)
	if err != nil {
		t.Fatalf("ListLockWaits: %v", err)
	}

	if len(waits) != 1 {
		t.Fatalf("got %d lock waits, want 1", len(waits))
	}

	if waits[0].WaitingPid != 10 || waits[0].BlockedByPid != 20 {
		t.Errorf("waiting/blocked pids = %d/%d, want 10/20", waits[0].WaitingPid, waits[0].BlockedByPid)
	}

	if waits[0].BlockingQuery != "UPDATE t SET x=1" {
		t.Errorf("blocking query = %q, want the blocker's UPDATE", waits[0].BlockingQuery)
	}
}

func TestDuplicateObservationsDoNotChangeReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, scope, seeded := txFixture(t, "tx-db-test-dupes")

	before, err := client.ListTransactions(ctx, scope, 0, "age", true, 50, 0)
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}

	ids := make([]uint64, len(before))
	for i, tx := range before {
		ids[i] = tx.ID
	}

	eventsBefore, err := client.ListTransactionEvents(ctx, scope, ids)
	if err != nil {
		t.Fatalf("ListTransactionEvents: %v", err)
	}

	if insertErr := client.InsertTransactionActivity(ctx, seeded); insertErr != nil {
		t.Fatalf("reinsert: %v", insertErr)
	}

	after, err := client.ListTransactions(ctx, scope, 0, "age", true, 50, 0)
	if err != nil {
		t.Fatalf("ListTransactions after: %v", err)
	}

	eventsAfter, err := client.ListTransactionEvents(ctx, scope, ids)
	if err != nil {
		t.Fatalf("ListTransactionEvents after: %v", err)
	}

	if len(after) != len(before) {
		t.Errorf("transactions %d -> %d after a duplicated batch, want unchanged", len(before), len(after))
	}

	for i := range before {
		if after[i] != before[i] {
			t.Errorf("transaction %d changed: %+v -> %+v", i, before[i], after[i])
		}
	}

	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("events %d -> %d after a duplicated batch, want unchanged",
			len(eventsBefore), len(eventsAfter))
	}

	for i := range eventsBefore {
		if eventsAfter[i].FirstSeenAt != eventsBefore[i].FirstSeenAt ||
			eventsAfter[i].LastSeenAt != eventsBefore[i].LastSeenAt ||
			eventsAfter[i].State != eventsBefore[i].State {
			t.Errorf("event %d changed: %+v -> %+v", i, eventsBefore[i], eventsAfter[i])
		}
	}
}
