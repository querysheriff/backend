//go:build integration

package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

const transactionQuery = "UPDATE orders SET status = $1 WHERE id = $2"

func seedTransaction(t *testing.T, serverName string, states []string, xactStart time.Time, base time.Time) {
	t.Helper()

	ctx := context.Background()
	stats := chstats.New(testdb.ClickHouse(t))
	id := chstats.TransactionID(serverName, 10, base, xactStart)

	rows := make([]chstats.TransactionActivity, len(states))
	for i, state := range states {
		rows[i] = chstats.TransactionActivity{
			TransactionID: id, ServerName: serverName, DatabaseName: "db", UserName: "app",
			ApplicationName: "checkout", Pid: 10, BackendStart: base, XactStart: xactStart,
			CollectedAt: base.Add(time.Duration(i+2) * time.Second), QueryStart: xactStart,
			Query: transactionQuery, QueryTags: map[string]string{"service": "checkout"},
			State: state,
		}
	}

	if err := stats.InsertTransactionActivity(ctx, rows); err != nil {
		t.Fatalf("seed transaction activity: %v", err)
	}
}

func queryTransaction(t *testing.T, serverName string, base time.Time) *querysheriffv1.Transaction {
	t.Helper()

	stats := chstats.New(testdb.ClickHouse(t))
	srv := server.NewActivityServer(stats, nil)

	resp, err := srv.ListTransactions(
		viewer(context.Background()),
		connect.NewRequest(&querysheriffv1.ListTransactionsRequest{
			ServerName:   serverName,
			DatabaseName: "db",
			From:         timestamppb.New(base.Add(-time.Minute)),
			To:           timestamppb.New(base.Add(time.Minute)),
			Limit:        10,
		}),
	)
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}

	transactions := resp.Msg.GetTransactions()
	if len(transactions) != 1 {
		t.Fatalf("got %d transactions, want 1", len(transactions))
	}

	return transactions[0]
}

func TestTransactionTimelineIsContiguousFromTheTransactionStart(t *testing.T) {
	t.Parallel()

	const serverName = "transaction-timeline"

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	xactStart := base.Add(time.Second)

	seedTransaction(t, serverName, []string{
		"active", "active", "idle in transaction", "idle in transaction (aborted)",
	}, xactStart, base)

	transaction := queryTransaction(t, serverName, base)

	events := transaction.GetEvents()
	if len(events) != 3 {
		t.Fatalf("got %d events, want the 4 observations collapsed into 3 runs", len(events))
	}

	if from := events[0].GetFrom().AsTime(); !from.Equal(xactStart) {
		t.Errorf("the first event starts at %s, want the transaction start %s", from, xactStart)
	}

	for i := 1; i < len(events); i++ {
		previous, current := events[i-1].GetTo().AsTime(), events[i].GetFrom().AsTime()
		if !previous.Equal(current) {
			t.Errorf("event %d ends at %s but event %d begins at %s, leaving a gap in the timeline",
				i-1, previous, i, current)
		}
	}

	last := events[len(events)-1]
	if to, end := last.GetTo().AsTime(), transaction.GetLastSeenAt().AsTime(); !to.Equal(end) {
		t.Errorf("the last event ends at %s, want the transaction end %s", to, end)
	}
}

func TestTransactionEventStatusFollowsTheBackendState(t *testing.T) {
	t.Parallel()

	const serverName = "transaction-status"

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	xactStart := base.Add(time.Second)

	seedTransaction(t, serverName, []string{
		"active", "idle in transaction", "idle in transaction (aborted)", "fastpath function call",
	}, xactStart, base)

	events := queryTransaction(t, serverName, base).GetEvents()

	want := []querysheriffv1.TransactionEventStatus{
		querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ACTIVE,
		querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_IDLE,
		querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ABORTED,
		querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ACTIVE,
	}

	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d", len(events), len(want))
	}

	for i, status := range want {
		if got := events[i].GetStatus(); got != status {
			t.Errorf("event %d status = %s, want %s", i, got, status)
		}
	}

	for i, event := range events {
		if event.GetQuery() != transactionQuery {
			t.Errorf("event %d query = %q, want it carried onto every run", i, event.GetQuery())
		}

		if event.GetQueryTags()["service"] != "checkout" {
			t.Errorf("event %d tags = %v, want the query tags carried onto every run", i, event.GetQueryTags())
		}
	}
}

func TestActivityRequiresServerAndDatabase(t *testing.T) {
	t.Parallel()

	srv := server.NewActivityServer(chstats.New(testdb.ClickHouse(t)), nil)
	from, to := timestamppb.New(time.Now().Add(-time.Hour)), timestamppb.Now()

	for name, request := range map[string]*querysheriffv1.ListTransactionsRequest{
		"no server":   {DatabaseName: "db", From: from, To: to},
		"no database": {ServerName: "some-server", From: from, To: to},
	} {
		_, err := srv.ListTransactions(viewer(context.Background()), connect.NewRequest(request))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: ListTransactions = %v, want invalid_argument", name, err)
		}
	}
}

func TestTransactionAgeSeriesPeaksAtTheTransactionsAge(t *testing.T) {
	t.Parallel()

	const serverName = "transaction-age-series"

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	seedTransaction(t, serverName, []string{"active", "active", "active", "active"}, base.Add(time.Second), base)

	srv := server.NewActivityServer(chstats.New(testdb.ClickHouse(t)), nil)

	resp, err := srv.GetTransactionAgeSeries(viewer(context.Background()),
		connect.NewRequest(&querysheriffv1.GetTransactionAgeSeriesRequest{
			ServerName:   serverName,
			DatabaseName: "db",
			From:         timestamppb.New(base.Add(-10 * time.Minute)),
			To:           timestamppb.New(base.Add(10 * time.Minute)),
		}))
	if err != nil {
		t.Fatalf("GetTransactionAgeSeries: %v", err)
	}

	var peak float64
	for _, point := range resp.Msg.GetAgeSeconds() {
		peak = max(peak, point.GetValue())
	}

	if peak != 4 {
		t.Errorf("peak age = %vs, want 4s: xact_start +1s, last seen +5s", peak)
	}
}
