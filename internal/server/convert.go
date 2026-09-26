package server

import (
	"math"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func timestamptzProto(ts pgtype.Timestamptz) *timestamppb.Timestamp {
	if !ts.Valid {
		return nil
	}

	return timestamppb.New(ts.Time)
}

func signedPid(pid uint32) int32 {
	if pid > math.MaxInt32 {
		return 0
	}

	return int32(pid)
}

func enumValues[E ~int32](values []E) []int32 {
	if len(values) == 0 {
		return nil
	}

	out := make([]int32, len(values))
	for i, v := range values {
		out[i] = int32(v)
	}

	return out
}

func orEmptyStrings(values []string) []string {
	if values == nil {
		return []string{}
	}

	return values
}
