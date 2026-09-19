package server

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/tagfilter"
)

// ListTagKeys returns available statement tag keys with distinct value counts.
// Example: -> [{Key:"env", ValueCount:3}, {Key:"team", ValueCount:5}].
func (s *StatementServer) ListTagKeys(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTagKeysRequest],
) (*connect.Response[querysheriffv1.ListTagKeysResponse], error) {
	msg := req.Msg

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if name := msg.GetServerName(); name != "" && !principal.CanViewServer(name) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	tags, err := s.stats.TagsInScope(ctx, msg.GetServerName(), msg.GetDatabaseName())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ListTagKeysResponse{Keys: tagKeysFrom(tags)}), nil
}

// ListTagValues returns values for one tag key with matching statement counts.
// Example: key="env" -> [{Value:"prod", StatementCount:20}, {Value:"dev", StatementCount:5}].
func (s *StatementServer) ListTagValues(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTagValuesRequest],
) (*connect.Response[querysheriffv1.ListTagValuesResponse], error) {
	msg := req.Msg

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if name := msg.GetServerName(); name != "" && !principal.CanViewServer(name) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	key := msg.GetKey()
	if !tagfilter.ValidKey(key) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", key))
	}

	tags, err := s.stats.TagsInScope(ctx, msg.GetServerName(), msg.GetDatabaseName())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ListTagValuesResponse{Values: tagValuesFor(tags, key)}), nil
}

func tagKeysFrom(tags []clickhouse.StatementTagSet) []*querysheriffv1.TagKey {
	values := map[string]map[string]bool{}
	statements := map[string]map[uint64]bool{}

	for _, set := range tags {
		for key, value := range set.Tags {
			if values[key] == nil {
				values[key] = map[string]bool{}
				statements[key] = map[uint64]bool{}
			}

			values[key][value] = true
			statements[key][set.StatementID] = true
		}
	}

	keys := make([]*querysheriffv1.TagKey, 0, len(values))
	for key, distinct := range values {
		keys = append(keys, &querysheriffv1.TagKey{Key: key, ValueCount: int64(len(distinct))})
	}

	sort.Slice(keys, func(i, j int) bool {
		left, right := len(statements[keys[i].GetKey()]), len(statements[keys[j].GetKey()])
		if left != right {
			return left > right
		}

		return keys[i].GetKey() < keys[j].GetKey()
	})

	return keys
}

func tagValuesFor(tags []clickhouse.StatementTagSet, key string) []*querysheriffv1.TagValue {
	statements := map[string]map[uint64]bool{}

	for _, set := range tags {
		value, present := set.Tags[key]
		if !present {
			continue
		}

		if statements[value] == nil {
			statements[value] = map[uint64]bool{}
		}

		statements[value][set.StatementID] = true
	}

	values := make([]*querysheriffv1.TagValue, 0, len(statements))
	for value, ids := range statements {
		values = append(values, &querysheriffv1.TagValue{Value: value, StatementCount: int64(len(ids))})
	}

	sort.Slice(values, func(i, j int) bool {
		if values[i].GetStatementCount() != values[j].GetStatementCount() {
			return values[i].GetStatementCount() > values[j].GetStatementCount()
		}

		return values[i].GetValue() < values[j].GetValue()
	})

	return values
}
