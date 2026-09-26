package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/gen/db"
)

type HealthServer struct {
	queries *db.Queries
}

func NewHealthServer(queries *db.Queries) *HealthServer {
	return &HealthServer{queries: queries}
}

// ReportHealth stores the latest collector health for its server.
// Example: databases=["app","postgres"] -> health updated for authenticated collector.
func (s *HealthServer) ReportHealth(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportHealthRequest],
) (*connect.Response[querysheriffv1.ReportHealthResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	err = s.queries.UpsertCollectorHealth(ctx, db.UpsertCollectorHealthParams{
		ServerName:  serverName,
		CollectedAt: pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true},
		Databases:   orEmptyStrings(msg.GetDatabases()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ReportHealthResponse{}), nil
}

// ListServers returns the servers that reported in the last 24 hours and the caller may view.
// Example: allowed=["prod"] -> only prod server is returned.
func (s *HealthServer) ListServers(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.ListServersRequest],
) (*connect.Response[querysheriffv1.ListServersResponse], error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := s.queries.ListMonitoredServers(ctx, principal.AllowedServerFilter())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	servers := make([]*querysheriffv1.Server, len(rows))
	for i, row := range rows {
		servers[i] = &querysheriffv1.Server{
			ServerName: row.ServerName,
			Databases:  row.Databases,
			LastSeenAt: timestamptzProto(row.CollectedAt),
		}
	}

	return connect.NewResponse(&querysheriffv1.ListServersResponse{Servers: servers}), nil
}
