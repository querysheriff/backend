package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	"github.com/querysheriff/backend/internal/clickhouse"
)

func (s *StatementServer) authorizedStatement(ctx context.Context, id uint64) (clickhouse.StatementDetail, error) {
	if id == 0 {
		return clickhouse.StatementDetail{}, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	detail, err := s.stats.StatementDetail(ctx, id)
	if err != nil {
		return clickhouse.StatementDetail{}, lookupError("statement", id, err)
	}

	return detail, authorizeServer(ctx, detail.ServerName)
}

func (s *StatementServer) authorizedSample(ctx context.Context, sampleID uint64) (clickhouse.Sample, error) {
	if sampleID == 0 {
		return clickhouse.Sample{}, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	detail, err := s.stats.Sample(ctx, sampleID)
	if err != nil {
		return clickhouse.Sample{}, lookupError("statement sample", sampleID, err)
	}

	return detail, authorizeServer(ctx, detail.ServerName)
}

func lookupError(what string, id uint64, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s %d not found", what, id))
	}

	return connect.NewError(connect.CodeInternal, err)
}
