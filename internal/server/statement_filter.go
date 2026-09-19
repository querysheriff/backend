package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/tagfilter"
)

type statementFilter struct {
	statementIDs []uint64
}

func tagFiltersFromProto(filters []*querysheriffv1.TagFilter) ([]tagfilter.Filter, error) {
	if len(filters) == 0 {
		return nil, nil
	}

	parsed := make([]tagfilter.Filter, len(filters))

	for i, tf := range filters {
		op, err := tagFilterOp(tf.GetOp())
		if err != nil {
			return nil, err
		}

		parsed[i] = tagfilter.Filter{Key: tf.GetKey(), Op: op, Values: tf.GetValues()}
	}

	if err := tagfilter.Validate(parsed); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	return parsed, nil
}

func tagFilterOp(op querysheriffv1.TagFilterOperator) (tagfilter.Op, error) {
	switch op {
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS:
		return tagfilter.OpExists, nil
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL:
		return tagfilter.OpEqual, nil
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL:
		return tagfilter.OpNotEqual, nil
	case querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_UNSPECIFIED:
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("tag filter op is required"))
	}

	return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown tag filter op"))
}

func (s *StatementServer) resolveStatementFilter(
	ctx context.Context,
	queryText string,
	filters []*querysheriffv1.TagFilter,
	serverName, databaseName pgtype.Text,
) (statementFilter, error) {
	byText, err := s.statementIDsMatchingText(ctx, queryText, serverName, databaseName)
	if err != nil {
		return statementFilter{}, err
	}

	byTag, err := s.statementIDsMatchingTags(ctx, filters, serverName, databaseName)
	if err != nil {
		return statementFilter{}, err
	}

	return statementFilter{statementIDs: intersectIDs(byText, byTag)}, nil
}

func (s *StatementServer) statementIDsMatchingText(
	ctx context.Context,
	queryText string,
	serverName, databaseName pgtype.Text,
) ([]uint64, error) {
	trimmed := strings.TrimSpace(queryText)
	if trimmed == "" {
		return nil, nil
	}

	ids, err := s.stats.StatementIDsByText(ctx, serverName.String, databaseName.String, trimmed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return ids, nil
}

func (s *StatementServer) statementIDsMatchingTags(
	ctx context.Context,
	filters []*querysheriffv1.TagFilter,
	serverName, databaseName pgtype.Text,
) ([]uint64, error) {
	parsed, err := tagFiltersFromProto(filters)
	if err != nil {
		return nil, err
	}

	if parsed == nil {
		return nil, nil
	}

	scoped, err := s.stats.TagsInScope(ctx, serverName.String, databaseName.String)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	ids := []uint64{}

	for _, set := range scoped {
		if tagfilter.Matches(set.Tags, parsed) {
			ids = append(ids, set.StatementID)
		}
	}

	return ids, nil
}

func intersectIDs(first, second []uint64) []uint64 {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	keep := make(map[uint64]bool, len(second))
	for _, id := range second {
		keep[id] = true
	}

	both := []uint64{}

	for _, id := range first {
		if keep[id] {
			both = append(both, id)
		}
	}

	return both
}

func requestedKinds(kinds []querysheriffv1.QueryKind) []int32 {
	out := make([]int32, len(kinds))
	for i, k := range kinds {
		out[i] = int32(k)
	}
	return out
}

func (s *StatementServer) listStatements(
	ctx context.Context,
	msg *querysheriffv1.QueryStatementsRequest,
	serverName, databaseName pgtype.Text,
	filter statementFilter,
) ([]*querysheriffv1.StatementStat, bool, error) {
	limit := resolveLimit(msg.GetLimit())
	rows, err := s.stats.ListStatementStats(ctx, clickhouse.ListStatementStatsParams{
		From:         msg.GetFrom().AsTime(),
		To:           msg.GetTo().AsTime(),
		ServerName:   serverName.String,
		DatabaseName: databaseName.String,
		StatementIDs: filter.statementIDs,
		Kinds:        requestedKinds(msg.GetKinds()),
		SortKey:      statementSortKey(msg.GetSortColumn()),
		SortDesc:     msg.GetSortDesc(),
		OffsetRows:   resolveOffset(msg.GetOffset()),
		RowLimit:     limit + 1,
	})
	if err != nil {
		return nil, false, err
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	ids := make([]uint64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}

	tagsByID, err := s.stats.StatementTags(ctx, ids)
	if err != nil {
		return nil, false, err
	}

	statements := make([]*querysheriffv1.StatementStat, len(rows))
	for i, row := range rows {
		tags, ok := tagsByID[row.ID]
		if !ok {
			tags = map[string]string{}
		}

		statements[i] = &querysheriffv1.StatementStat{
			Id:            row.ID,
			Preview:       row.Preview,
			UserName:      row.UserName,
			TotalExecTime: row.TotalExecTime,
			PctOfTotal:    row.PctOfTotal,
			Calls:         row.Calls,
			AvgExecTime:   avgExecTime(row.TotalExecTime, row.Calls),
			Rows:          row.Rows,
			Tags:          tags,
			PctIo:         row.PctIo,
		}
	}

	return statements, hasMore, nil
}

const sortKeyPctTime = "pct_time"

func statementSortKey(col querysheriffv1.StatementSortColumn) string {
	switch col {
	case querysheriffv1.StatementSortColumn_STATEMENT_SORT_COLUMN_AVG:
		return "avg"
	case querysheriffv1.StatementSortColumn_STATEMENT_SORT_COLUMN_CALLS:
		return "calls"
	case querysheriffv1.StatementSortColumn_STATEMENT_SORT_COLUMN_ROWS_PER_CALL:
		return "rows_per_call"
	case querysheriffv1.StatementSortColumn_STATEMENT_SORT_COLUMN_PCT_IO:
		return "pct_io"
	case querysheriffv1.StatementSortColumn_STATEMENT_SORT_COLUMN_PCT_TIME,
		querysheriffv1.StatementSortColumn_STATEMENT_SORT_COLUMN_UNSPECIFIED:
		return sortKeyPctTime
	}

	return sortKeyPctTime
}

func sampleSortKey(col querysheriffv1.SampleSortColumn) string {
	switch col {
	case querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_DURATION:
		return "duration"
	case querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_PLAN:
		return "plan"
	case querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_AT,
		querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_UNSPECIFIED:
		return "at"
	}

	return "at"
}
