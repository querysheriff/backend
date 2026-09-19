package server

import (
	"context"
	"math"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func timestamptzProto(ts pgtype.Timestamptz) *timestamppb.Timestamp {
	if !ts.Valid {
		return nil
	}

	return timestamppb.New(ts.Time)
}

func listAndDecode[Row any, Record any](
	ctx context.Context,
	list func(context.Context) ([]Row, error),
	decode func(Row) (Record, error),
) ([]Record, error) {
	rows, err := list(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	records := make([]Record, len(rows))
	for i, row := range rows {
		record, decodeErr := decode(row)
		if decodeErr != nil {
			return nil, connect.NewError(connect.CodeInternal, decodeErr)
		}

		records[i] = record
	}

	return records, nil
}

func signedPid(pid uint32) int32 {
	if pid > math.MaxInt32 {
		return 0
	}

	return int32(pid)
}
