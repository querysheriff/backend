package server_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/querysheriff/backend/internal/server"
)

func TestLogInterceptorLevels(t *testing.T) {
	t.Parallel()

	gone, leave := context.WithCancel(t.Context())
	leave()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want string
	}{
		{"success is silent", t.Context(), nil, ""},
		{
			"internal is an error",
			t.Context(),
			connect.NewError(connect.CodeInternal, errors.New("db down")),
			"level=ERROR",
		},
		{"plain error is an error", t.Context(), errors.New("boom"), "level=ERROR"},
		{
			"bad input is debug",
			t.Context(),
			connect.NewError(connect.CodeInvalidArgument, errors.New("bad")),
			"level=DEBUG",
		},
		{"canceled is debug", t.Context(), connect.NewError(connect.CodeCanceled, context.Canceled), "level=DEBUG"},
		{
			"internal after the client went away is a canceled debug",
			gone,
			connect.NewError(
				connect.CodeInternal,
				context.Canceled,
			),
			"level=DEBUG msg=\"rpc failed\" procedure=\"\" code=canceled",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
			next := func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) { return nil, c.err }

			_, _ = server.NewLogInterceptor(logger)(next)(c.ctx, connect.NewRequest(&emptypb.Empty{}))

			if got := out.String(); c.want == "" && got != "" || !strings.Contains(got, c.want) {
				t.Errorf("logged %q, want it to contain %q", got, c.want)
			}
		})
	}
}
