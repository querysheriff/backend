//go:build integration

package alerts_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/testdb"
)

type slackMessage struct {
	Attachments []struct {
		Title  string `json:"title"`
		Text   string `json:"text"`
		Footer string `json:"footer"`
	} `json:"attachments"`
}

func TestLiveAlertMessages(t *testing.T) {
	t.Parallel()

	const serverName = "live-alert-texts"

	ctx := context.Background()
	queries := db.New(testdb.Postgres(t))

	var (
		mu       sync.Mutex
		messages = map[string]slackMessage{}
	)

	slack := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		var message slackMessage
		if err := json.Unmarshal(body, &message); err == nil && len(message.Attachments) == 1 {
			mu.Lock()
			messages[message.Attachments[0].Title] = message
			mu.Unlock()
		}
	}))
	defer slack.Close()

	if err := queries.UpsertAlertWebhook(ctx, db.UpsertAlertWebhookParams{
		ServerName: serverName, SlackWebhookUrl: slack.URL,
	}); err != nil {
		t.Fatalf("UpsertAlertWebhook: %v", err)
	}

	now := time.Now()
	ago := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(-d)) }

	notifier := alerts.NewNotifier(queries, slog.New(slog.DiscardHandler))
	notifier.CheckActivity(serverName, now, []*querysheriffv1.ActivitySnapshot{
		{
			Pid:           10,
			DatabaseName:  "shop",
			State:         "active",
			XactStart:     ago(time.Minute / 2),
			QueryStart:    ago(time.Minute / 2),
			Query:         "UPDATE servers SET state = $1 WHERE id = $2",
			QueryTags:     map[string]string{"gql_operation": "Provision"},
			BlockedByPid:  20,
			LockWaitStart: ago(time.Minute / 2),
		},
		{
			Pid: 20, DatabaseName: "shop", State: "idle in transaction", XactStart: ago(15 * time.Minute),
			QueryStart: ago(15 * time.Minute), Query: "SELECT * FROM servers WHERE load < 5 FOR UPDATE",
			QueryTags: map[string]string{"job": "reconcile"},
		},
		{
			Pid:          30,
			DatabaseName: "shop",
			State:        "active",
			XactStart:    ago(2 * time.Minute),
			QueryStart:   ago(2 * time.Minute),
			Query:        "SELECT pg_sleep(600)",
		},
	})
	notifier.CheckLogs(serverName, []*querysheriffv1.LogEvent{{
		LogLevel: querysheriffv1.LogEvent_LOG_LEVEL_PANIC, Message: "crashed <!channel> & more",
	}})
	notifier.Wait()

	mu.Lock()
	defer mu.Unlock()

	if got := messages["🔥 Database crashed"].Attachments; len(got) != 1 ||
		got[0].Text != "crashed &lt;!channel&gt; &amp; more" {
		t.Errorf("crash alert = %+v, want the log message escaped for Slack", got)
	}

	cases := map[string][]string{
		"Query blocked by a lock": {
			"A query has been waiting more than 10 seconds for a lock.",
			"*Waiting:* `gql_operation=Provision`\n```UPDATE servers SET state = $1 WHERE id = $2```",
			"*Blocking:* `job=reconcile`\n```SELECT * FROM servers WHERE load &lt; 5 FOR UPDATE```",
			"*Kill blocking:* `SELECT pg_terminate_backend(20);`",
		},
		"Query running too long": {
			"A query has been running more than 1 minute.",
			"*Query:*\n```SELECT pg_sleep(600)```",
			"*Kill:* `SELECT pg_terminate_backend(30);`",
		},
		"Transaction open too long": {
			"A transaction has been open more than 10 minutes.",
			"*Current query:* `job=reconcile`\n```SELECT * FROM servers WHERE load &lt; 5 FOR UPDATE```",
			"*Kill:* `SELECT pg_terminate_backend(20);`",
		},
	}

	for title, want := range cases {
		message, ok := messages["🔥 "+title]
		if !ok {
			t.Errorf("no %q message sent; got %v", title, messages)

			continue
		}

		attachment := message.Attachments[0]
		if !strings.HasPrefix(attachment.Text, want[0]) {
			t.Errorf("%s starts %q, want %q", title, attachment.Text, want[0])
		}

		for _, part := range want[1:] {
			if !strings.Contains(attachment.Text, part) {
				t.Errorf("%s text %q is missing %q", title, attachment.Text, part)
			}
		}

		if attachment.Footer != serverName+" · shop" {
			t.Errorf("%s footer = %q, want the server and database", title, attachment.Footer)
		}
	}
}
