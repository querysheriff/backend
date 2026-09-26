package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/auth"
	"github.com/querysheriff/backend/internal/gen/db"
)

const (
	invalidCredentialsMsg = "invalid email or password"
	pgUniqueViolation     = "23505"
)

type AuthServer struct {
	queries      *db.Queries
	cookieSecure bool
}

func NewAuthServer(queries *db.Queries, cookieSecure bool) *AuthServer {
	return &AuthServer{queries: queries, cookieSecure: cookieSecure}
}

// Login authenticates a user, creates a session, and sets the session cookie.
// Example: email+password -> user + Set-Cookie.
func (s *AuthServer) Login(
	ctx context.Context,
	req *connect.Request[querysheriffv1.LoginRequest],
) (*connect.Response[querysheriffv1.LoginResponse], error) {
	email := strings.ToLower(strings.TrimSpace(req.Msg.GetEmail()))
	password := req.Msg.GetPassword()
	if email == "" || password == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("email and password are required"))
	}

	principal, err := s.authenticate(ctx, email, password)
	if err != nil {
		return nil, err
	}

	token, err := auth.GenerateToken(auth.SessionTokenPrefix)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	err = s.queries.CreateSession(ctx, db.CreateSessionParams{
		TokenHash: auth.HashToken(token),
		UserID:    principal.UserID,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(sessionTTL), Valid: true},
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := connect.NewResponse(&querysheriffv1.LoginResponse{User: userProto(principal)})
	resp.Header().Set("Set-Cookie", sessionCookie(token, int(sessionTTL.Seconds()), s.cookieSecure).String())

	return resp, nil
}

// Logout deletes the current session and clears the session cookie.
// Example: valid session cookie -> session removed.
func (s *AuthServer) Logout(
	ctx context.Context,
	req *connect.Request[querysheriffv1.LogoutRequest],
) (*connect.Response[querysheriffv1.LogoutResponse], error) {
	if token := sessionTokenFromHeader(req.Header()); token != "" {
		if err := s.queries.DeleteSession(ctx, auth.HashToken(token)); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	resp := connect.NewResponse(&querysheriffv1.LogoutResponse{})
	resp.Header().Set("Set-Cookie", sessionCookie("", -1, s.cookieSecure).String())

	return resp, nil
}

// GetCurrentUser returns the authenticated user.
// Example: logged-in Bob -> {Name:"Bob", Email:"bob@example.com"}.
func (s *AuthServer) GetCurrentUser(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.GetCurrentUserRequest],
) (*connect.Response[querysheriffv1.GetCurrentUserResponse], error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.GetCurrentUserResponse{User: userProto(principal)}), nil
}

func (s *AuthServer) authenticate(ctx context.Context, email, password string) (*auth.Principal, error) {
	user, err := s.queries.GetUserByEmail(ctx, email)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.bootstrapSuperAdmin(ctx, email, password)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if !auth.CheckPassword(user.PasswordHash, password) {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New(invalidCredentialsMsg))
	}

	return principalFromUser(user), nil
}

func (s *AuthServer) bootstrapSuperAdmin(ctx context.Context, email, password string) (*auth.Principal, error) {
	count, err := s.queries.CountUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if count > 0 {
		auth.CheckPassword(auth.DecoyHash, password)

		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New(invalidCredentialsMsg))
	}

	hash, err := hashPassword(password)
	if err != nil {
		return nil, err
	}

	created, err := s.queries.CreateUser(ctx, db.CreateUserParams{
		Name:           defaultNameFromEmail(email),
		Email:          email,
		PasswordHash:   hash,
		IsSuperAdmin:   true,
		AllowedServers: []string{},
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New(invalidCredentialsMsg))
		}

		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return principalFromUser(created), nil
}

func principalFromUser(user db.User) *auth.Principal {
	return &auth.Principal{
		UserID:         user.ID,
		Name:           user.Name,
		Email:          user.Email,
		IsSuperAdmin:   user.IsSuperAdmin,
		CreatedAt:      user.CreatedAt.Time,
		AllowedServers: user.AllowedServers,
	}
}

func userProto(principal *auth.Principal) *querysheriffv1.User {
	return &querysheriffv1.User{
		Id:             principal.UserID,
		Name:           principal.Name,
		Email:          principal.Email,
		IsSuperAdmin:   principal.IsSuperAdmin,
		CreatedAt:      timestamppb.New(principal.CreatedAt),
		AllowedServers: principal.AllowedServers,
	}
}

func defaultNameFromEmail(email string) string {
	if local, _, ok := strings.Cut(email, "@"); ok && local != "" {
		return local
	}

	return email
}

func hashPassword(password string) (string, error) {
	hash, err := auth.HashPassword(password)
	if errors.Is(err, bcrypt.ErrPasswordTooLong) {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("password must be at most 72 bytes"))
	}
	if err != nil {
		return "", connect.NewError(connect.CodeInternal, err)
	}

	return hash, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}
