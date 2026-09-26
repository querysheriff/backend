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

	interceptors := connect.WithInterceptors(server.NewAuthInterceptor(queries, false))

	mux := http.NewServeMux()

	healthPath, healthHandler := querysheriffv1connect.NewHealthServiceHandler(
		server.NewHealthServer(queries), interceptors)
	mux.Handle(healthPath, healthHandler)

	adminPath, adminHandler := querysheriffv1connect.NewAdminServiceHandler(
		server.NewAdminServer(pool), interceptors)
	mux.Handle(adminPath, adminHandler)

	authPath, authHandler := querysheriffv1connect.NewAuthServiceHandler(
		server.NewAuthServer(queries, false), interceptors)
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

	req := connect.NewRequest(&querysheriffv1.GetCurrentUserRequest{})
	header(req.Header())

	_, err := client.GetCurrentUser(context.Background(), req)

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

func TestEveryProcedureKindDemandsItsOwnCredentials(t *testing.T) {
	t.Parallel()

	f := newAuthFixture(t)

	cases := []struct {
		name   string
		call   func(func(http.Header)) error
		header func(http.Header)
		want   connect.Code
	}{
		{"ReportHealth with a collector token", f.reportHealth, bearer(f.collectorToken), codeOK},
		{"ReportHealth without credentials", f.reportHealth, noHeader, connect.CodeUnauthenticated},
		{"ReportHealth with an unknown token", f.reportHealth, bearer("qsc_nope"), connect.CodeUnauthenticated},
		{"ReportHealth with an empty bearer", f.reportHealth, bearer(""), connect.CodeUnauthenticated},
		{"ReportHealth with an admin session", f.reportHealth, cookie(f.adminCookie), connect.CodeUnauthenticated},
		{"CurrentUser with a user session", f.currentUser, cookie(f.viewerCookie), codeOK},
		{"CurrentUser with an admin session", f.currentUser, cookie(f.adminCookie), codeOK},
		{"CurrentUser without credentials", f.currentUser, noHeader, connect.CodeUnauthenticated},
		{"CurrentUser with a collector token", f.currentUser, bearer(f.collectorToken), connect.CodeUnauthenticated},
		{"ListUsers with an admin session", f.listUsers, cookie(f.adminCookie), codeOK},
		{"ListUsers with a user session", f.listUsers, cookie(f.viewerCookie), connect.CodePermissionDenied},
		{"ListUsers without credentials", f.listUsers, noHeader, connect.CodeUnauthenticated},
		{
			"ListUsers with an unknown session", f.listUsers, cookie("querysheriff_session=qss_nope"),
			connect.CodeUnauthenticated,
		},
		{"ListUsers with a collector token", f.listUsers, bearer(f.collectorToken), connect.CodeUnauthenticated},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assertCode(t, c.name, c.call(c.header), c.want)
		})
	}
}

func TestPasswordChangeEndsTheUsersSessions(t *testing.T) {
	t.Parallel()

	f := newAuthFixture(t)
	ctx := context.Background()
	admin := querysheriffv1connect.NewAdminServiceClient(http.DefaultClient, f.url)

	listReq := connect.NewRequest(&querysheriffv1.ListUsersRequest{})
	cookie(f.adminCookie)(listReq.Header())

	users, err := admin.ListUsers(ctx, listReq)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}

	var viewer *querysheriffv1.User

	for _, user := range users.Msg.GetUsers() {
		if user.GetEmail() == f.viewerEmail {
			viewer = user
		}
	}

	if viewer == nil {
		t.Fatalf("viewer %s not listed", f.viewerEmail)
	}

	update := func(password string) {
		req := connect.NewRequest(&querysheriffv1.UpdateUserRequest{
			Id: viewer.GetId(), Name: viewer.GetName(), Email: viewer.GetEmail(), Password: password,
		})
		cookie(f.adminCookie)(req.Header())

		if _, updateErr := admin.UpdateUser(ctx, req); updateErr != nil {
			t.Fatalf("UpdateUser: %v", updateErr)
		}
	}

	update("")
	assertCode(t, "CurrentUser after a profile-only update", f.currentUser(cookie(f.viewerCookie)), codeOK)

	update("a-brand-new-passphrase")
	assertCode(t, "CurrentUser after a password change",
		f.currentUser(cookie(f.viewerCookie)), connect.CodeUnauthenticated)
}

//nolint:paralleltest // Changes the shared super admin's password; parallel tests would lose their admin sessions.
func TestChangingYourOwnPasswordKeepsYourSession(t *testing.T) {
	f := newAuthFixture(t)
	otherBrowser := newAuthFixture(t).adminCookie
	ctx := context.Background()
	admin := querysheriffv1connect.NewAdminServiceClient(http.DefaultClient, f.url)

	listReq := connect.NewRequest(&querysheriffv1.ListUsersRequest{})
	cookie(f.adminCookie)(listReq.Header())

	users, err := admin.ListUsers(ctx, listReq)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}

	var self *querysheriffv1.User

	for _, user := range users.Msg.GetUsers() {
		if user.GetIsSuperAdmin() {
			self = user
		}
	}

	if self == nil {
		t.Fatal("no super admin listed")
	}

	req := connect.NewRequest(&querysheriffv1.UpdateUserRequest{
		Id: self.GetId(), Name: self.GetName(), Email: self.GetEmail(), Password: seededPassword,
	})
	cookie(f.adminCookie)(req.Header())

	if _, err = admin.UpdateUser(ctx, req); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}

	assertCode(t, "CurrentUser in the session that changed the password", f.currentUser(cookie(f.adminCookie)), codeOK)
	assertCode(t, "CurrentUser in another session of the same user",
		f.currentUser(cookie(otherBrowser)), connect.CodeUnauthenticated)
}
