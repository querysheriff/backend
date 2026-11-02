package server

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/tagfilter"
)

// GetStatementTags returns the tags of one statement's samples in the range, every value of each key.
// Example: samples tagged op=A and op=B -> [{op A} {op B}].
func (s *StatementServer) GetStatementTags(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementTagsRequest],
) (*connect.Response[querysheriffv1.GetStatementTagsResponse], error) {
	msg := req.Msg

	scope, err := authorizedTagScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	tags, err := s.stats.StatementTags(ctx, scope, msg.GetStatementId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.GetStatementTagsResponse{Tags: tagsProto(tags)}), nil
}

// ListTagKeys returns the statement tag keys in the range with distinct value counts.
// Example: -> [{Key:"env", ValueCount:3}, {Key:"team", ValueCount:5}].
func (s *StatementServer) ListTagKeys(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTagKeysRequest],
) (*connect.Response[querysheriffv1.ListTagKeysResponse], error) {
	msg := req.Msg

	scope, err := authorizedTagScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.stats.TagKeys(ctx, scope)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	keys := make([]*querysheriffv1.TagKey, len(rows))
	for i, row := range rows {
		keys[i] = &querysheriffv1.TagKey{Key: row.Key, ValueCount: row.ValueCount}
	}

	return connect.NewResponse(&querysheriffv1.ListTagKeysResponse{Keys: keys}), nil
}

// ListTagValues returns values for one tag key in the range with matching statement counts.
// Example: key="env" -> [{Value:"prod", StatementCount:20}, {Value:"dev", StatementCount:5}].
func (s *StatementServer) ListTagValues(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTagValuesRequest],
) (*connect.Response[querysheriffv1.ListTagValuesResponse], error) {
	msg := req.Msg

	scope, err := authorizedTagScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	key := msg.GetKey()
	if !tagfilter.ValidKey(key) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", key))
	}

	rows, err := s.stats.TagValues(ctx, scope, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	values := make([]*querysheriffv1.TagValue, len(rows))
	for i, row := range rows {
		values[i] = &querysheriffv1.TagValue{Value: row.Value, StatementCount: row.StatementCount}
	}

	return connect.NewResponse(&querysheriffv1.ListTagValuesResponse{Values: values}), nil
}

func authorizedTagScope(
	ctx context.Context,
	serverName, databaseName string,
	from, to *timestamppb.Timestamp,
) (clickhouse.TagScope, error) {
	if err := authorizeDatabaseQuery(ctx, serverName, databaseName, from, to); err != nil {
		return clickhouse.TagScope{}, err
	}

	return clickhouse.TagScope{
		ServerName:   serverName,
		DatabaseName: databaseName,
		From:         from.AsTime(),
		To:           to.AsTime(),
	}, nil
}

// tagsProto flattens tags to key=value pairs sorted by key, then value.
// Example: {op:[B, A]} -> [{op A} {op B}].
func tagsProto(tags map[string][]string) []*querysheriffv1.Tag {
	var out []*querysheriffv1.Tag

	for _, key := range slices.Sorted(maps.Keys(tags)) {
		for _, value := range slices.Sorted(slices.Values(tags[key])) {
			out = append(out, &querysheriffv1.Tag{Key: key, Value: value})
		}
	}

	return out
}
