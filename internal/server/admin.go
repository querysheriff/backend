package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/auth"
	"github.com/querysheriff/backend/internal/gen/db"
)

const emailExistsMsg = "a user with that email already exists"

type AdminServer struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewAdminServer(pool *pgxpool.Pool) *AdminServer {
	return &AdminServer{pool: pool, queries: db.New(pool)}
}

// ListCollectorTokens returns all collector tokens.
// Example: -> [{ID:1, ServerName:"prod"}, {ID:2, ServerName:"staging"}].
func (s *AdminServer) ListCollectorTokens(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.ListCollectorTokensRequest],
) (*connect.Response[querysheriffv1.ListCollectorTokensResponse], error) {
	rows, err := s.queries.ListCollectorTokens(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tokens := make([]*querysheriffv1.CollectorToken, len(rows))
	for i, row := range rows {
		tokens[i] = &querysheriffv1.CollectorToken{
			Id:         row.ID,
			ServerName: row.ServerName,
			CreatedAt:  timestamptzProto(row.CreatedAt),
		}
	}

	return connect.NewResponse(&querysheriffv1.ListCollectorTokensResponse{Tokens: tokens}), nil
}

// CreateCollectorToken creates a collector token for a server.
// Example: serverName="prod" -> token "qsc_..." for prod.
func (s *AdminServer) CreateCollectorToken(
	ctx context.Context,
	req *connect.Request[querysheriffv1.CreateCollectorTokenRequest],
) (*connect.Response[querysheriffv1.CreateCollectorTokenResponse], error) {
	serverName := strings.TrimSpace(req.Msg.GetServerName())
	if serverName == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("server_name is required"))
	}

	token, err := auth.GenerateToken(auth.CollectorTokenPrefix)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	row, err := s.queries.CreateCollectorToken(ctx, db.CreateCollectorTokenParams{
		ServerName: serverName,
		TokenHash:  auth.HashToken(token),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.CreateCollectorTokenResponse{
		Token: &querysheriffv1.CollectorToken{
			Id:         row.ID,
			ServerName: row.ServerName,
			CreatedAt:  timestamptzProto(row.CreatedAt),
		},
		TokenValue: token,
	}), nil
}

// DeleteCollectorToken deletes a collector token. Deleting a server's last token removes the server together with its health, alert config and user access.
func (s *AdminServer) DeleteCollectorToken(
	ctx context.Context,
	req *connect.Request[querysheriffv1.DeleteCollectorTokenRequest],
) (*connect.Response[querysheriffv1.DeleteCollectorTokenResponse], error) {
	id := req.Msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("token id is required"))
	}

	err := s.inTx(ctx, func(q *db.Queries) error {
		serverName, err := q.DeleteCollectorToken(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		return q.DeleteServerIfUnused(ctx, serverName)
	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.DeleteCollectorTokenResponse{}), nil
}

// ListUsers returns all users.
// Example: -> [{ID:1, Email:"admin@example.com"}, ...].
func (s *AdminServer) ListUsers(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.ListUsersRequest],
) (*connect.Response[querysheriffv1.ListUsersResponse], error) {
	rows, err := s.queries.ListUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	users := make([]*querysheriffv1.User, len(rows))
	for i, row := range rows {
		users[i] = userProto(principalFromUser(row.User, row.AllowedServers))
	}

	return connect.NewResponse(&querysheriffv1.ListUsersResponse{Users: users}), nil
}

// CreateUser creates a non-super-admin user.
// Example: name="Bob", email="bob@example.com" -> new User.
func (s *AdminServer) CreateUser(
	ctx context.Context,
	req *connect.Request[querysheriffv1.CreateUserRequest],
) (*connect.Response[querysheriffv1.CreateUserResponse], error) {
	name := strings.TrimSpace(req.Msg.GetName())
	email := strings.ToLower(strings.TrimSpace(req.Msg.GetEmail()))
	password := req.Msg.GetPassword()
	if name == "" || email == "" || password == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name, email and password are required"))
	}

	hash, err := hashPassword(password)
	if err != nil {
		return nil, err
	}

	var created db.GetUserByIDRow

	err = s.inTx(ctx, func(q *db.Queries) error {
		user, createErr := q.CreateUser(ctx, db.CreateUserParams{
			Name:         name,
			Email:        email,
			PasswordHash: hash,
			IsSuperAdmin: false,
		})
		if isUniqueViolation(createErr) {
			return connect.NewError(connect.CodeAlreadyExists, errors.New(emailExistsMsg))
		}
		if createErr != nil {
			return createErr
		}

		created, createErr = setUserServers(ctx, q, user.ID, req.Msg.GetAllowedServers())

		return createErr
	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.CreateUserResponse{
		User: userProto(principalFromUser(created.User, created.AllowedServers)),
	}), nil
}

// UpdateUser updates a user's profile, server access, and optionally password.
// Example: id=2, name="Bob", password="" -> updates user without changing password.
func (s *AdminServer) UpdateUser(
	ctx context.Context,
	req *connect.Request[querysheriffv1.UpdateUserRequest],
) (*connect.Response[querysheriffv1.UpdateUserResponse], error) {
	id := req.Msg.GetId()
	name := strings.TrimSpace(req.Msg.GetName())
	email := strings.ToLower(strings.TrimSpace(req.Msg.GetEmail()))
	if id == 0 || name == "" || email == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user id, name and email are required"))
	}

	passwordHash := pgtype.Text{String: "", Valid: false}
	if password := req.Msg.GetPassword(); password != "" {
		hash, hashErr := hashPassword(password)
		if hashErr != nil {
			return nil, hashErr
		}

		passwordHash = pgtype.Text{String: hash, Valid: true}
	}

	var updated db.GetUserByIDRow

	err := s.inTx(ctx, func(q *db.Queries) error {
		rows, err := q.UpdateUser(ctx, db.UpdateUserParams{
			Name:              name,
			Email:             email,
			PasswordHash:      passwordHash,
			ID:                id,
			CallerSessionHash: auth.HashToken(sessionTokenFromHeader(req.Header())),
		})
		if isUniqueViolation(err) {
			return connect.NewError(connect.CodeAlreadyExists, errors.New(emailExistsMsg))
		}
		if err != nil {
			return err
		}
		if rows == 0 {
			return connect.NewError(connect.CodeNotFound, errors.New("user not found"))
		}

		updated, err = setUserServers(ctx, q, id, req.Msg.GetAllowedServers())

		return err
	})
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.UpdateUserResponse{
		User: userProto(principalFromUser(updated.User, updated.AllowedServers)),
	}), nil
}

// DeleteUser deletes a non-super-admin user.
// Example: id=2 -> user deleted; super admin -> error.
func (s *AdminServer) DeleteUser(
	ctx context.Context,
	req *connect.Request[querysheriffv1.DeleteUserRequest],
) (*connect.Response[querysheriffv1.DeleteUserResponse], error) {
	id := req.Msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user id is required"))
	}

	row, err := s.queries.GetUserByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("user not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if row.User.IsSuperAdmin {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the super admin cannot be deleted"))
	}

	if err = s.queries.DeleteUser(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.DeleteUserResponse{}), nil
}

// inTx runs fn in a transaction.
func (s *AdminServer) inTx(ctx context.Context, fn func(q *db.Queries) error) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error { return fn(s.queries.WithTx(tx)) })

	var connectErr *connect.Error
	if err == nil || errors.As(err, &connectErr) {
		return err
	}

	return connect.NewError(connect.CodeInternal, err)
}

// setUserServers replaces a user's server access and returns the user as stored.
func setUserServers(ctx context.Context, q *db.Queries, userID int64, serverNames []string) (db.GetUserByIDRow, error) {
	err := q.SetUserServers(ctx, db.SetUserServersParams{UserID: userID, ServerNames: serverNames})
	if isForeignKeyViolation(err) {
		return db.GetUserByIDRow{}, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("allowed_servers lists an unknown server"),
		)
	}
	if err != nil {
		return db.GetUserByIDRow{}, err
	}

	return q.GetUserByID(ctx, userID)
}
