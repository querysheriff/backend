//go:build integration

package server_test

import (
	"context"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/auth"
)

func tagFilter(key string, op querysheriffv1.TagFilterOperator, values ...string) *querysheriffv1.TagFilter {
	return &querysheriffv1.TagFilter{Key: key, Op: op, Values: values}
}

func viewer(ctx context.Context) context.Context {
	return auth.WithPrincipal(ctx, &auth.Principal{UserID: 1, Email: "viewer@dev.dev", IsSuperAdmin: true})
}

func collector(ctx context.Context, serverName string) context.Context {
	return auth.WithServerName(ctx, serverName)
}
