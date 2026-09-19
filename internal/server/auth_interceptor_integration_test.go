//go:build integration

package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/gen/querysheriff/v1/querysheriffv1connect"
	"github.com/querysheriff/backend/internal/auth"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

const codeOK = connect.Code(0)

const seededPassword = "s3cret-passphrase"

type authFixture struct {
	url            string
	collectorToken string
	adminCookie    string
	viewerCookie   string
	viewerEmail    string
}

func newAuthFixture(t *testing.T) authFixture {
	t.Helper()

	ctx := context.Background()
	pool := testdb.Postgres(t)
	queries := db.New(pool)
	unique := time.Now().UnixNano()

	interceptors := connect.WithInterceptors(server.NewAuthInterceptor(queries))

	mux := http.NewServeMux()

	healthPath, healthHandler := querysheriffv1connect.NewHealthServiceHandler(
		server.NewHealthServer(queries), interceptors)
	mux.Handle(healthPath, healthHandler)

	adminPath, adminHandler := querysheriffv1connect.NewAdminServiceHandler(
		server.NewAdminServer(pool), interceptors)
	mux.Handle(adminPath, adminHandler)

	authPath, authHandler := querysheriffv1connect.NewAuthServiceHandler(
		server.NewAuthServer(pool, false), interceptors)
	mux.Handle(authPath, authHandler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return authFixture{
		url:            srv.URL,
		collectorToken: seedCollectorToken(ctx, t, queries, fmt.Sprintf("auth-fixture-%d", unique)),
		adminCookie:    seedSession(ctx, t, queries, unique, "admin", true),
		viewerCookie:   seedSession(ctx, t, queries, unique, "viewer", false),
		viewerEmail:    userEmail("viewer", unique),
	}
}

func userEmail(role string, unique int64) string {
	return fmt.Sprintf("%s-%d@dev.dev", role, unique)
}

func seedCollectorToken(ctx context.Context, t *testing.T, queries *db.Queries, serverName string) string {
	t.Helper()

	token, err := auth.GenerateToken(auth.CollectorTokenPrefix)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	if _, err = queries.CreateCollectorToken(ctx, db.CreateCollectorTokenParams{
		ServerName: serverName,
		TokenHash:  auth.HashToken(token),
	}); err != nil {
		t.Fatalf("CreateCollectorToken: %v", err)
	}

	return token
}

func seedSession(ctx context.Context, t *testing.T, queries *db.Queries, unique int64, role string, admin bool) string {
	t.Helper()

	hash, err := auth.HashPassword(seededPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	userID, err := createUser(ctx, queries, db.CreateUserParams{
		Name:           role,
		Email:          userEmail(role, unique),
		PasswordHash:   hash,
		IsSuperAdmin:   admin,
		AllowedServers: []string{},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	token, err := auth.GenerateToken("qss_")
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	if err = queries.CreateSession(ctx, db.CreateSessionParams{
		TokenHash: auth.HashToken(token),
		UserID:    userID,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	return "querysheriff_session=" + token
}

// The schema allows a single super admin, so tests sharing a database reuse whichever one exists.
func createUser(ctx context.Context, queries *db.Queries, params db.CreateUserParams) (int64, error) {
	user, err := queries.CreateUser(ctx, params)
	if err == nil {
		return user.ID, nil
	}

	if !params.IsSuperAdmin {
		return 0, err
	}

	existing, listErr := queries.ListUsers(ctx)
	if listErr != nil {
		return 0, listErr
	}

	for _, candidate := range existing {
		if candidate.IsSuperAdmin {
			return candidate.ID, nil
		}
	}

	return 0, err
}

func (f authFixture) reportHealth(header func(http.Header)) error {
	client := querysheriffv1connect.NewHealthServiceClient(http.DefaultClient, f.url)

	req := connect.NewRequest(&querysheriffv1.ReportHealthRequest{
		CollectedAt: timestamppb.Now(),
		Databases:   []string{"shop"},
	})
	header(req.Header())

	_, err := client.ReportHealth(context.Background(), req)

	return err
}

func (f authFixture) listUsers(header func(http.Header)) error {
	client := querysheriffv1connect.NewAdminServiceClient(http.DefaultClient, f.url)

	req := connect.NewRequest(&querysheriffv1.ListUsersRequest{})
	header(req.Header())

	_, err := client.ListUsers(context.Background(), req)

	return err
}

func (f authFixture) currentUser(header func(http.Header)) error {
	client := querysheriffv1connect.NewAuthServiceClient(http.DefaultClient, f.url)

	req := connect.NewRequest(&querysheriffv1.CurrentUserRequest{})
	header(req.Header())

	_, err := client.CurrentUser(context.Background(), req)

	return err
}

func noHeader(http.Header) {}

func TestLoginIsReachableWithoutCredentials(t *testing.T) {
	t.Parallel()

	f := newAuthFixture(t)
	client := querysheriffv1connect.NewAuthServiceClient(http.DefaultClient, f.url)

	resp, err := client.Login(context.Background(), connect.NewRequest(&querysheriffv1.LoginRequest{
		Email:    f.viewerEmail,
		Password: seededPassword,
	}))
	if err != nil {
		t.Fatalf("Login without a session: %v", err)
	}

	if got := resp.Msg.GetUser().GetEmail(); got != f.viewerEmail {
		t.Errorf("Login returned %q, want %q", got, f.viewerEmail)
	}
}

func assertCode(t *testing.T, call string, err error, want connect.Code) {
	t.Helper()

	if want == codeOK {
		if err != nil {
			t.Errorf("%s = %v, want it to be accepted", call, err)
		}

		return
	}

	if got := connect.CodeOf(err); got != want {
		t.Errorf("%s = %v, want %v", call, got, want)
	}
}

func bearer(token string) func(http.Header) {
	return func(h http.Header) { h.Set("Authorization", "Bearer "+token) }
}

func cookie(value string) func(http.Header) {
	return func(h http.Header) { h.Set("Cookie", value) }
}

func TestCollectorProceduresRequireACollectorToken(t *testing.T) {
	t.Parallel()

	f := newAuthFixture(t)

	cases := []struct {
		name   string
		header func(http.Header)
		want   connect.Code
	}{
		{"a valid collector token", bearer(f.collectorToken), codeOK},
		{"no credentials at all", noHeader, connect.CodeUnauthenticated},
		{"an unknown token", bearer("qsc_not-a-real-token"), connect.CodeUnauthenticated},
		{"an empty bearer", bearer(""), connect.CodeUnauthenticated},
		{"a super admin session cookie", cookie(f.adminCookie), connect.CodeUnauthenticated},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assertCode(t, "ReportHealth with "+c.name, f.reportHealth(c.header), c.want)
		})
	}
}

func TestUserProceduresAcceptAnyLoggedInUser(t *testing.T) {
	t.Parallel()

	f := newAuthFixture(t)

	cases := []struct {
		name   string
		header func(http.Header)
		want   connect.Code
	}{
		{"a plain user session", cookie(f.viewerCookie), codeOK},
		{"a super admin session", cookie(f.adminCookie), codeOK},
		{"no credentials at all", noHeader, connect.CodeUnauthenticated},
		{"a collector token", bearer(f.collectorToken), connect.CodeUnauthenticated},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assertCode(t, "CurrentUser with "+c.name, f.currentUser(c.header), c.want)
		})
	}
}

func TestAdminProceduresRequireASuperAdminSession(t *testing.T) {
	t.Parallel()

	f := newAuthFixture(t)

	cases := []struct {
		name   string
		header func(http.Header)
		want   connect.Code
	}{
		{"a super admin session", cookie(f.adminCookie), codeOK},
		{"a plain user session", cookie(f.viewerCookie), connect.CodePermissionDenied},
		{"no credentials at all", noHeader, connect.CodeUnauthenticated},
		{"an unknown session", cookie("querysheriff_session=qss_nope"), connect.CodeUnauthenticated},
		{"a collector token", bearer(f.collectorToken), connect.CodeUnauthenticated},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assertCode(t, "ListUsers with "+c.name, f.listUsers(c.header), c.want)
		})
	}
}
