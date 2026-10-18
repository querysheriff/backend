//go:build integration

package server_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/auth"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

const testWebhook = "https://hooks.slack.com/services/T000/B000/secret"

type adminFixture struct {
	queries *db.Queries
	admin   *server.AdminServer
	alerts  *server.AlertServer
	health  *server.HealthServer
	unique  int64
}

func newAdminFixture(t *testing.T) adminFixture {
	t.Helper()

	pool := testdb.Postgres(t)
	queries := db.New(pool)

	return adminFixture{
		queries: queries,
		admin:   server.NewAdminServer(pool),
		alerts:  server.NewAlertServer(queries),
		health:  server.NewHealthServer(queries),
		unique:  time.Now().UnixNano(),
	}
}

func (f adminFixture) name(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, f.unique)
}

func (f adminFixture) createToken(ctx context.Context, t *testing.T, serverName string) int64 {
	t.Helper()

	resp, err := f.admin.CreateCollectorToken(ctx, connect.NewRequest(&querysheriffv1.CreateCollectorTokenRequest{
		ServerName: serverName,
	}))
	if err != nil {
		t.Fatalf("CreateCollectorToken(%s): %v", serverName, err)
	}

	return resp.Msg.GetToken().GetId()
}

func (f adminFixture) deleteToken(ctx context.Context, id int64) error {
	_, err := f.admin.DeleteCollectorToken(ctx, connect.NewRequest(&querysheriffv1.DeleteCollectorTokenRequest{Id: id}))

	return err
}

func (f adminFixture) reportHealth(ctx context.Context, t *testing.T, serverName string) {
	t.Helper()

	if err := f.queries.UpsertCollectorHealth(ctx, db.UpsertCollectorHealthParams{
		ServerName:  serverName,
		CollectedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Databases:   []string{"shop"},
	}); err != nil {
		t.Fatalf("UpsertCollectorHealth(%s): %v", serverName, err)
	}
}

func (f adminFixture) setWebhook(ctx context.Context, serverName, webhook string) error {
	_, err := f.alerts.UpdateAlertWebhook(ctx, connect.NewRequest(&querysheriffv1.UpdateAlertWebhookRequest{
		ServerName: serverName, SlackWebhookUrl: webhook,
	}))

	return err
}

func (f adminFixture) listedServers(ctx context.Context, t *testing.T) []string {
	t.Helper()

	resp, err := f.health.ListServers(ctx, connect.NewRequest(&querysheriffv1.ListServersRequest{}))
	if err != nil {
		t.Fatalf("ListServers: %v", err)
	}

	names := make([]string, 0, len(resp.Msg.GetServers()))
	for _, srv := range resp.Msg.GetServers() {
		names = append(names, srv.GetServerName())
	}

	return names
}

func (f adminFixture) webhook(ctx context.Context, t *testing.T, serverName string) (string, bool) {
	t.Helper()

	resp, err := f.alerts.ListAlertSettings(ctx, connect.NewRequest(&querysheriffv1.ListAlertSettingsRequest{}))
	if err != nil {
		t.Fatalf("ListAlertSettings: %v", err)
	}

	for _, settings := range resp.Msg.GetServers() {
		if settings.GetServerName() == serverName {
			return settings.GetSlackWebhookUrl(), true
		}
	}

	return "", false
}

func (f adminFixture) user(ctx context.Context, t *testing.T, email string) (*querysheriffv1.User, bool) {
	t.Helper()

	resp, err := f.admin.ListUsers(ctx, connect.NewRequest(&querysheriffv1.ListUsersRequest{}))
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}

	for _, user := range resp.Msg.GetUsers() {
		if user.GetEmail() == email {
			return user, true
		}
	}

	return nil, false
}

func TestDeletingTheLastTokenRemovesTheServerEverywhere(t *testing.T) {
	t.Parallel()

	ctx := viewer(context.Background())
	f := newAdminFixture(t)
	serverName := f.name("last-token")
	email := f.name("last-token") + "@dev.dev"

	first := f.createToken(ctx, t, serverName)
	f.reportHealth(ctx, t, serverName)

	if err := f.setWebhook(ctx, serverName, testWebhook); err != nil {
		t.Fatalf("UpdateAlertWebhook: %v", err)
	}

	second := f.createToken(ctx, t, serverName)

	if _, err := f.admin.CreateUser(ctx, connect.NewRequest(&querysheriffv1.CreateUserRequest{
		Name: "Bob", Email: email, Password: seededPassword, AllowedServers: []string{serverName},
	})); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if err := f.deleteToken(ctx, first); err != nil {
		t.Fatalf("DeleteCollectorToken(first): %v", err)
	}

	if webhook, _ := f.webhook(ctx, t, serverName); webhook != testWebhook {
		t.Errorf("webhook after deleting one of two tokens = %q, want it kept", webhook)
	}

	if user, _ := f.user(ctx, t, email); !slices.Equal(user.GetAllowedServers(), []string{serverName}) {
		t.Errorf(
			"allowed servers after deleting one of two tokens = %v, want [%s]",
			user.GetAllowedServers(),
			serverName,
		)
	}

	if err := f.deleteToken(ctx, second); err != nil {
		t.Fatalf("DeleteCollectorToken(second): %v", err)
	}

	if slices.Contains(f.listedServers(ctx, t), serverName) {
		t.Errorf("%s still listed after its last token was deleted", serverName)
	}

	if _, listed := f.webhook(ctx, t, serverName); listed {
		t.Errorf("%s still has alert settings after its last token was deleted", serverName)
	}

	if user, _ := f.user(ctx, t, email); len(user.GetAllowedServers()) != 0 {
		t.Errorf("allowed servers = %v, want the deleted server gone", user.GetAllowedServers())
	}

	if err := f.setWebhook(ctx, serverName, testWebhook); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("UpdateAlertWebhook on a deleted server = %v, want NotFound", err)
	}

	if err := f.deleteToken(ctx, second); err != nil {
		t.Errorf("deleting an already deleted token = %v, want OK", err)
	}
}

func TestUserServerAccessOnlyNamesKnownServers(t *testing.T) {
	t.Parallel()

	ctx := viewer(context.Background())
	f := newAdminFixture(t)
	alpha, beta := f.name("access-alpha"), f.name("access-beta")
	email := f.name("access") + "@dev.dev"

	f.createToken(ctx, t, alpha)
	f.createToken(ctx, t, beta)

	_, err := f.admin.CreateUser(ctx, connect.NewRequest(&querysheriffv1.CreateUserRequest{
		Name: "Ann", Email: email, Password: seededPassword, AllowedServers: []string{alpha, f.name("no-such-server")},
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("CreateUser with an unknown server = %v, want InvalidArgument", err)
	}

	if _, exists := f.user(ctx, t, email); exists {
		t.Fatalf("a rejected CreateUser left user %s behind", email)
	}

	created, err := f.admin.CreateUser(ctx, connect.NewRequest(&querysheriffv1.CreateUserRequest{
		Name: "Ann", Email: email, Password: seededPassword, AllowedServers: []string{beta, alpha, beta},
	}))
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if got := created.Msg.GetUser().GetAllowedServers(); !slices.Equal(got, []string{alpha, beta}) {
		t.Errorf("created allowed servers = %v, want [%s %s] deduplicated and sorted", got, alpha, beta)
	}

	updated, err := f.admin.UpdateUser(ctx, connect.NewRequest(&querysheriffv1.UpdateUserRequest{
		Id: created.Msg.GetUser().GetId(), Name: "Ann", Email: email,
	}))
	if err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}

	if got := updated.Msg.GetUser().GetAllowedServers(); len(got) != 0 {
		t.Errorf("allowed servers after clearing = %v, want none", got)
	}

	_, err = f.admin.UpdateUser(ctx, connect.NewRequest(&querysheriffv1.UpdateUserRequest{
		Id: created.Msg.GetUser().GetId(), Name: "Ann", Email: email, AllowedServers: []string{f.name("gone")},
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("UpdateUser with an unknown server = %v, want InvalidArgument", err)
	}

	_, err = f.admin.UpdateUser(ctx, connect.NewRequest(&querysheriffv1.UpdateUserRequest{
		Id: 1 << 62, Name: "Nobody", Email: f.name("nobody") + "@dev.dev",
	}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("UpdateUser of a missing user = %v, want NotFound", err)
	}
}

func TestUserWithoutServersSeesNone(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t)
	admin := viewer(context.Background())
	serverName := f.name("hidden")

	f.createToken(admin, t, serverName)
	f.reportHealth(admin, t, serverName)

	nobody := auth.WithPrincipal(context.Background(), &auth.Principal{UserID: 2, Email: "nobody@dev.dev"})

	if servers := f.listedServers(nobody, t); len(servers) != 0 {
		t.Errorf("ListServers for a user without servers = %v, want none", servers)
	}

	if _, listed := f.webhook(nobody, t, serverName); listed {
		t.Errorf("ListAlertSettings shows %s to a user without servers", serverName)
	}
}

func TestAlertConfigForUnknownServerIsNotFound(t *testing.T) {
	t.Parallel()

	ctx := viewer(context.Background())
	f := newAdminFixture(t)
	unknown := f.name("unknown")

	if err := f.setWebhook(ctx, unknown, testWebhook); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("UpdateAlertWebhook = %v, want NotFound", err)
	}

	_, err := f.alerts.UpdateAlertSetting(ctx, connect.NewRequest(&querysheriffv1.UpdateAlertSettingRequest{
		ServerName: unknown, Key: alerts.KeyPanic, Enabled: false,
	}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("UpdateAlertSetting = %v, want NotFound", err)
	}

	known := f.name("cleared-webhook")
	f.createToken(ctx, t, known)
	f.reportHealth(ctx, t, known)

	for _, webhook := range []string{testWebhook, ""} {
		if err = f.setWebhook(ctx, known, webhook); err != nil {
			t.Fatalf("UpdateAlertWebhook(%q): %v", webhook, err)
		}
	}

	if webhook, listed := f.webhook(ctx, t, known); !listed || webhook != "" {
		t.Errorf("cleared webhook = %q (listed %v), want an empty webhook", webhook, listed)
	}
}
