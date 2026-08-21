package auth

import (
	"context"
	"slices"
	"time"
)

type Principal struct {
	UserID         int64
	Name           string
	Email          string
	IsSuperAdmin   bool
	CreatedAt      time.Time
	AllowedServers []string
}

// AllowedServerFilter is nil for a super admin (every server matches); an empty set matches nothing.
func (p *Principal) AllowedServerFilter() []string {
	if p == nil || p.IsSuperAdmin {
		return nil
	}

	return p.AllowedServers
}

func (p *Principal) CanViewServer(serverName string) bool {
	if p == nil {
		return false
	}

	if p.IsSuperAdmin {
		return true
	}

	return slices.Contains(p.AllowedServers, serverName)
}

type contextKey int

const (
	serverNameKey contextKey = iota
	principalKey
)

func WithServerName(ctx context.Context, serverName string) context.Context {
	return context.WithValue(ctx, serverNameKey, serverName)
}

func ServerNameFromContext(ctx context.Context) (string, bool) {
	serverName, ok := ctx.Value(serverNameKey).(string)

	return serverName, ok
}

func WithPrincipal(ctx context.Context, principal *Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalKey).(*Principal)

	return principal, ok
}
