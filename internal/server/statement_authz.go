package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *StatementServer) authorizeStatementID(ctx context.Context, id uint64) error {
	serverName, _, err := s.stats.StatementScope(ctx, id)
	if err != nil {
		return statementLookupError(id, err)
	}

	return s.authorizeServer(ctx, serverName)
}

func (s *StatementServer) authorizeSampleID(ctx context.Context, sampleID uint64) error {
	serverName, err := s.stats.SampleServer(ctx, sampleID)
	if err != nil {
		return sampleLookupError(sampleID, err)
	}

	return s.authorizeServer(ctx, serverName)
}

func (s *StatementServer) authorizeServer(ctx context.Context, serverName string) error {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return err
	}

	if !principal.CanViewServer(serverName) {
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("access to that server is not allowed"))
	}

	return nil
}

func (s *StatementServer) authorizeStatementQuery(
	ctx context.Context,
	serverName, databaseName string,
	from, to *timestamppb.Timestamp,
) error {
	if serverName == "" || databaseName == "" {
		return connect.NewError(connect.CodeInvalidArgument,
			errors.New("server_name and database_name are required"))
	}

	if err := s.authorizeServer(ctx, serverName); err != nil {
		return err
	}

	return requireRange(from, to)
}

func sampleLookupError(sampleID uint64, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("statement sample %d not found", sampleID))
	}

	return connect.NewError(connect.CodeInternal, err)
}

func statementLookupError(id uint64, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("statement %d not found", id))
	}

	return connect.NewError(connect.CodeInternal, err)
}
