package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
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
	if err = validateTagKey(key); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
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

const (
	maxTagFilters    = 20
	maxTagValues     = 50
	maxTagValueBytes = 256
)

// tagFiltersFromProto validates a request's tag filters and converts them to ClickHouse filters.
func tagFiltersFromProto(filters []*querysheriffv1.TagFilter) ([]clickhouse.TagFilter, error) {
	if len(filters) == 0 {
		return nil, nil
	}

	if len(filters) > maxTagFilters {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("at most %d tag filters are allowed", maxTagFilters))
	}

	parsed := make([]clickhouse.TagFilter, len(filters))

	for i, filter := range filters {
		op, err := tagFilterOp(filter.GetOp())
		if err != nil {
			return nil, err
		}

		if err = validateTagFilter(filter.GetKey(), op, filter.GetValues()); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}

		parsed[i] = clickhouse.TagFilter{Key: filter.GetKey(), Op: op, Values: filter.GetValues()}
	}

	return parsed, nil
}

func tagFilterOp(op querysheriffv1.TagFilterOperator) (clickhouse.TagFilterOp, error) {
	switch op {
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS:
		return clickhouse.TagExists, nil
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL:
		return clickhouse.TagEqual, nil
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL:
		return clickhouse.TagNotEqual, nil
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_UNSPECIFIED:
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("tag filter op is required"))
	}

	return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown tag filter op"))
}

func validateTagFilter(key string, op clickhouse.TagFilterOp, values []string) error {
	if err := validateTagKey(key); err != nil {
		return err
	}

	if op == clickhouse.TagExists {
		if len(values) != 0 {
			return errors.New("an exists tag filter must not carry values")
		}

		return nil
	}

	if len(values) == 0 {
		return errors.New("tag filter requires at least one value")
	}

	if len(values) > maxTagValues {
		return fmt.Errorf("a tag filter accepts at most %d values", maxTagValues)
	}

	for _, value := range values {
		if value == "" {
			return errors.New("tag filter values must not be empty")
		}

		if len(value) > maxTagValueBytes {
			return fmt.Errorf("tag filter values must be at most %d bytes", maxTagValueBytes)
		}
	}

	return nil
}

// validateTagKey checks a tag key against ^[a-z][a-z0-9_]*$.
// Example: "server_name" -> nil, "Server-Name" -> error.
func validateTagKey(key string) error {
	valid := key != "" && key[0] >= 'a' && key[0] <= 'z'
	for i := 1; valid && i < len(key); i++ {
		c := key[i]
		valid = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'
	}

	if !valid {
		return fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", key)
	}

	return nil
}
