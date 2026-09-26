package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/tagfilter"
)

// ListTagKeys returns available statement tag keys with distinct value counts.
// Example: -> [{Key:"env", ValueCount:3}, {Key:"team", ValueCount:5}].
func (s *StatementServer) ListTagKeys(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTagKeysRequest],
) (*connect.Response[querysheriffv1.ListTagKeysResponse], error) {
	msg := req.Msg

	if err := authorizeDatabase(ctx, msg.GetServerName(), msg.GetDatabaseName()); err != nil {
		return nil, err
	}

	rows, err := s.stats.TagKeys(ctx, msg.GetServerName(), msg.GetDatabaseName())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	keys := make([]*querysheriffv1.TagKey, len(rows))
	for i, row := range rows {
		keys[i] = &querysheriffv1.TagKey{Key: row.Key, ValueCount: row.ValueCount}
	}

	return connect.NewResponse(&querysheriffv1.ListTagKeysResponse{Keys: keys}), nil
}

// ListTagValues returns values for one tag key with matching statement counts.
// Example: key="env" -> [{Value:"prod", StatementCount:20}, {Value:"dev", StatementCount:5}].
func (s *StatementServer) ListTagValues(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTagValuesRequest],
) (*connect.Response[querysheriffv1.ListTagValuesResponse], error) {
	msg := req.Msg

	if err := authorizeDatabase(ctx, msg.GetServerName(), msg.GetDatabaseName()); err != nil {
		return nil, err
	}

	key := msg.GetKey()
	if !tagfilter.ValidKey(key) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", key))
	}

	rows, err := s.stats.TagValues(ctx, msg.GetServerName(), msg.GetDatabaseName(), key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	values := make([]*querysheriffv1.TagValue, len(rows))
	for i, row := range rows {
		values[i] = &querysheriffv1.TagValue{Value: row.Value, StatementCount: row.StatementCount}
	}

	return connect.NewResponse(&querysheriffv1.ListTagValuesResponse{Values: values}), nil
}
