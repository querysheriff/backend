package server

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"
)

// NewLogInterceptor logs failed RPCs, using error for server faults and debug for client faults.
func NewLogInterceptor(logger *slog.Logger) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()

			resp, err := next(ctx, req)
			if err != nil {
				code := connect.CodeOf(err)
				logger.Log(ctx, failureLevel(code), "rpc failed",
					"procedure", req.Spec().Procedure,
					"code", code.String(),
					"duration", time.Since(start),
					"error", err,
				)
			}

			return resp, err
		}
	}
}

func failureLevel(code connect.Code) slog.Level {
	switch code {
	case connect.CodeInternal,
		connect.CodeUnknown,
		connect.CodeUnavailable,
		connect.CodeDataLoss,
		connect.CodeDeadlineExceeded:
		return slog.LevelError
	case connect.CodeCanceled,
		connect.CodeInvalidArgument,
		connect.CodeNotFound,
		connect.CodeAlreadyExists,
		connect.CodePermissionDenied,
		connect.CodeResourceExhausted,
		connect.CodeFailedPrecondition,
		connect.CodeAborted,
		connect.CodeOutOfRange,
		connect.CodeUnimplemented,
		connect.CodeUnauthenticated:
		return slog.LevelDebug
	}

	return slog.LevelError
}
