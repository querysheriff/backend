package server_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/auth"
	"github.com/querysheriff/backend/internal/server"
)

const (
	maxTagFilters    = 20
	maxTagValues     = 50
	maxTagValueBytes = 256
	unknownTagOp     = 99
)

const (
	tagUnspecified = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_UNSPECIFIED
	tagExists      = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS
	tagEqual       = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL
	tagNotEqual    = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL
)

// Malformed tag input is rejected before any query runs, so these servers need no ClickHouse.
func TestListStatementsRejectsMalformedTagFilters(t *testing.T) {
	t.Parallel()

	filter := func(key string, op querysheriffv1.TagFilterOperator, values ...string) *querysheriffv1.TagFilter {
		return &querysheriffv1.TagFilter{Key: key, Op: op, Values: values}
	}

	tooManyFilters := slices.Repeat([]*querysheriffv1.TagFilter{filter("service", tagExists)}, maxTagFilters+1)

	cases := map[string][]*querysheriffv1.TagFilter{
		"missing op":                {filter("service", tagUnspecified, "a")},
		"unknown op":                {filter("service", unknownTagOp, "a")},
		"exists with a value":       {filter("service", tagExists, "a")},
		"equal without a value":     {filter("service", tagEqual)},
		"not equal without a value": {filter("service", tagNotEqual)},
		"empty value":               {filter("service", tagEqual, "")},
		"oversized value":           {filter("service", tagEqual, strings.Repeat("x", maxTagValueBytes+1))},
		"too many values":           {filter("service", tagEqual, slices.Repeat([]string{"a"}, maxTagValues+1)...)},
		"too many filters":          tooManyFilters,
		"valid filter before a bad one": {
			filter("service", tagEqual, "checkout"),
			filter("Service", tagExists),
		},
	}

	for _, key := range invalidTagKeys() {
		cases["key "+key] = []*querysheriffv1.TagFilter{filter(key, tagExists)}
	}

	srv := server.NewStatementServer(nil)
	from, to := lastHour()

	for name, filters := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := srv.ListStatements(superAdmin(), connect.NewRequest(&querysheriffv1.ListStatementsRequest{
				ServerName:   "srv",
				DatabaseName: "db",
				From:         from,
				To:           to,
				TagFilters:   filters,
			}))

			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("ListStatements(%v) = %v, want an invalid_argument error", filters, err)
			}
		})
	}
}

func TestListTagValuesRejectsMalformedKeys(t *testing.T) {
	t.Parallel()

	srv := server.NewStatementServer(nil)
	from, to := lastHour()

	for _, key := range invalidTagKeys() {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			_, err := srv.ListTagValues(superAdmin(), connect.NewRequest(&querysheriffv1.ListTagValuesRequest{
				ServerName:   "srv",
				DatabaseName: "db",
				From:         from,
				To:           to,
				Key:          key,
			}))

			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("ListTagValues(key=%q) = %v, want an invalid_argument error", key, err)
			}
		})
	}
}

func invalidTagKeys() []string {
	return []string{"", "Service", "2service", "_service", "ser-vice", "a.b", "a b", "ä"}
}

func superAdmin() context.Context {
	return auth.WithPrincipal(
		context.Background(),
		&auth.Principal{UserID: 1, Email: "admin@dev.dev", IsSuperAdmin: true},
	)
}

func lastHour() (*timestamppb.Timestamp, *timestamppb.Timestamp) {
	now := time.Now()

	return timestamppb.New(now.Add(-time.Hour)), timestamppb.New(now)
}
