package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultQueryLimit = 50
	maxQueryLimit     = 1000
)

func authorizeServer(ctx context.Context, serverName string) error {
	if serverName == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("server_name is required"))
	}

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return err
	}

	if !principal.CanViewServer(serverName) {
		return connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	return nil
}

func authorizeDatabase(ctx context.Context, serverName, databaseName string) error {
	if databaseName == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("database_name is required"))
	}

	return authorizeServer(ctx, serverName)
}

func authorizeServerQuery(ctx context.Context, serverName string, from, to *timestamppb.Timestamp) error {
	if err := authorizeServer(ctx, serverName); err != nil {
		return err
	}

	return requireRange(from, to)
}

func authorizeDatabaseQuery(
	ctx context.Context,
	serverName, databaseName string,
	from, to *timestamppb.Timestamp,
) error {
	if err := authorizeDatabase(ctx, serverName, databaseName); err != nil {
		return err
	}

	return requireRange(from, to)
}

func requireTimestamp(ts *timestamppb.Timestamp) error {
	if ts != nil {
		return nil
	}

	return connect.NewError(connect.CodeInvalidArgument, errors.New("collected_at is required"))
}

func requireRange(from, to *timestamppb.Timestamp) error {
	if from == nil || to == nil || !to.AsTime().After(from.AsTime()) {
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("from and to are required, and to must be after from"),
		)
	}

	return nil
}

func resolveLimit(limit int32) int32 {
	if limit <= 0 {
		return defaultQueryLimit
	}

	return min(limit, maxQueryLimit)
}

// trimPage drops the extra row fetched to learn whether another page follows.
func trimPage[T any](rows []T, limit int32) ([]T, bool) {
	if len(rows) > int(limit) {
		return rows[:limit], true
	}

	return rows, false
}

func resolveOffset(offset int32) int32 {
	if offset < 0 {
		return 0
	}

	return offset
}
