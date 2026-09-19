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

// AllowedServerFilter returns allowed servers, or nil for unrestricted access.
// Example: admin -> nil; user -> ["prod-1", "prod-2"].
func (p *Principal) AllowedServerFilter() []string {
	if p == nil || p.IsSuperAdmin {
		return nil
	}

	return p.AllowedServers
}

// CanViewServer reports whether the principal may access a server.
// Example: AllowedServers=["prod-1"], "prod-1" -> true.
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

// WithServerName stores a server name in context.
// Example: WithServerName(ctx, "prod-1").
func WithServerName(ctx context.Context, serverName string) context.Context {
	return context.WithValue(ctx, serverNameKey, serverName)
}

// ServerNameFromContext returns the server name stored in context.
// Example: "prod-1" -> ("prod-1", true).
func ServerNameFromContext(ctx context.Context) (string, bool) {
	serverName, ok := ctx.Value(serverNameKey).(string)

	return serverName, ok
}

// WithPrincipal stores a principal in context.
// Example: WithPrincipal(ctx, user).
func WithPrincipal(ctx context.Context, principal *Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

// PrincipalFromContext returns the principal stored in context.
// Example: stored user -> (user, true).
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalKey).(*Principal)

	return principal, ok
}
