package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/sqltext"
	"github.com/querysheriff/backend/internal/tagfilter"
)

type StatementServer struct {
	stats *clickhouse.Client
}

func NewStatementServer(stats *clickhouse.Client) *StatementServer {
	return &StatementServer{stats: stats}
}

// ReportStatements stores statement metric deltas and returns statements missing query text.
// Example: 3 deltas, 1 unknown statement -> UnknownStatements=[...].
func (s *StatementServer) ReportStatements(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportStatementsRequest],
) (*connect.Response[querysheriffv1.ReportStatementsResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	deltas := msg.GetStatementDeltas()
	if len(deltas) == 0 {
		return connect.NewResponse(&querysheriffv1.ReportStatementsResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	collectedAt := msg.GetCollectedAt().AsTime()

	statementIDs := make([]uint64, len(deltas))
	for i, delta := range deltas {
		statementIDs[i] = clickhouse.StatementID(
			serverName, delta.GetDatabaseName(), delta.GetUserName(), delta.GetQueryId())
	}

	known, err := s.stats.StatementsWithText(ctx, statementIDs)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	rows := make([]clickhouse.StatementDelta, len(deltas))

	var (
		unknown  []*querysheriffv1.StatementIdentity
		mirrored []clickhouse.Statement
	)

	for i, delta := range deltas {
		rows[i] = clickhouse.StatementDelta{
			CollectedAt:   collectedAt,
			ServerName:    serverName,
			DatabaseName:  delta.GetDatabaseName(),
			StatementID:   statementIDs[i],
			Calls:         delta.GetCalls(),
			Rows:          delta.GetRows(),
			TotalExecTime: delta.GetTotalExecTime(),
			TotalIoTime:   delta.GetTotalIoTime(),
		}

		if known[statementIDs[i]] {
			continue
		}

		unknown = append(unknown, &querysheriffv1.StatementIdentity{
			UserName:     delta.GetUserName(),
			DatabaseName: delta.GetDatabaseName(),
			QueryId:      delta.GetQueryId(),
		})
		mirrored = append(mirrored, clickhouse.Statement{
			ID:           statementIDs[i],
			ServerName:   serverName,
			DatabaseName: delta.GetDatabaseName(),
			UserName:     delta.GetUserName(),
			QueryID:      delta.GetQueryId(),
		})
	}

	if err = s.stats.UpsertStatements(ctx, mirrored); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err = s.stats.InsertStatementDeltas(ctx, rows); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ReportStatementsResponse{UnknownStatements: unknown}), nil
}

// ReportStatementTexts stores normalized full/short query text and query kind.
// Example: "SELECT  * FROM users" -> Clean="SELECT * FROM users", Kind=READS.
func (s *StatementServer) ReportStatementTexts(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportStatementTextsRequest],
) (*connect.Response[querysheriffv1.ReportStatementTextsResponse], error) {
	texts := req.Msg.GetStatementTexts()
	if len(texts) == 0 {
		return connect.NewResponse(&querysheriffv1.ReportStatementTextsResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	filled := make([]clickhouse.Statement, len(texts))
	for i, text := range texts {
		identity := text.GetIdentity()
		summary := sqltext.Process(text.GetQuery())
		filled[i] = clickhouse.Statement{
			ID: clickhouse.StatementID(serverName, identity.GetDatabaseName(),
				identity.GetUserName(), identity.GetQueryId()),
			ServerName:   serverName,
			DatabaseName: identity.GetDatabaseName(),
			UserName:     identity.GetUserName(),
			QueryID:      identity.GetQueryId(),
			QueryShort:   summary.Preview,
			QueryFull:    summary.Clean,
			QueryKind:    int32(summary.Kind),
		}
	}

	if err = s.stats.UpsertStatements(ctx, filled); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ReportStatementTextsResponse{}), nil
}

// ListStatements returns filtered, sorted, paginated statement statistics.
// Example: server=prod, db=app, limit=50 -> up to 50 matching statements.
func (s *StatementServer) ListStatements(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListStatementsRequest],
) (*connect.Response[querysheriffv1.ListStatementsResponse], error) {
	msg := req.Msg

	if err := authorizeDatabaseQuery(
		ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo()); err != nil {
		return nil, err
	}

	tagFilters, err := tagFiltersFromProto(msg.GetTagFilters())
	if err != nil {
		return nil, err
	}

	var statementIDs []uint64 // nil matches every statement

	if text := strings.TrimSpace(msg.GetSearch()); text != "" {
		statementIDs, err = s.stats.StatementIDsByText(ctx, msg.GetServerName(), msg.GetDatabaseName(), text)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.stats.ListStatementStats(ctx, clickhouse.ListStatementStatsParams{
		From:         msg.GetFrom().AsTime(),
		To:           msg.GetTo().AsTime(),
		ServerName:   msg.GetServerName(),
		DatabaseName: msg.GetDatabaseName(),
		StatementIDs: statementIDs,
		TagFilters:   tagFilters,
		Kinds:        enumValues(msg.GetKinds()),
		SortKey:      statementSortKey(msg.GetSortColumn()),
		SortDesc:     msg.GetSortDesc(),
		OffsetRows:   resolveOffset(msg.GetOffset()),
		RowLimit:     limit + 1,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	rows, hasMore := trimPage(rows, limit)

	statements := make([]*querysheriffv1.StatementStat, len(rows))
	for i, row := range rows {
		statements[i] = &querysheriffv1.StatementStat{
			Id:       row.ID,
			Preview:  row.Preview,
			UserName: row.UserName,
			Tags:     row.Tags,
			Calls:    row.Calls,
			Rows:     row.Rows,
			AvgMs:    avgExecTime(row.TotalExecTime, row.Calls),
			PctTime:  row.PctOfTotal,
			PctIo:    row.PctIo,
		}
	}

	return connect.NewResponse(&querysheriffv1.ListStatementsResponse{
		Statements: statements,
		HasMore:    hasMore,
	}), nil
}

// GetStatement returns one statement's full query text, scope, and tags.
// Example: id=42 -> {Query:"SELECT ...", ServerName:"prod", DatabaseName:"app", Tags:{...}}.
func (s *StatementServer) GetStatement(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementRequest],
) (*connect.Response[querysheriffv1.GetStatementResponse], error) {
	detail, err := s.authorizedStatement(ctx, req.Msg.GetId())
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.GetStatementResponse{
		Query:        detail.Query,
		ServerName:   detail.ServerName,
		DatabaseName: detail.DatabaseName,
		Tags:         detail.Tags,
	}), nil
}

// ListStatementSamples returns filtered, sorted, paginated samples for one statement.
// Example: statement 42, limit=50 -> up to 50 execution samples.
func (s *StatementServer) ListStatementSamples(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListStatementSamplesRequest],
) (*connect.Response[querysheriffv1.ListStatementSamplesResponse], error) {
	msg := req.Msg

	from, to := msg.GetFrom(), msg.GetTo()
	if err := authorizeDatabaseQuery(ctx, msg.GetServerName(), msg.GetDatabaseName(), from, to); err != nil {
		return nil, err
	}

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.stats.ListStatementSamples(ctx, clickhouse.ListSamplesParams{
		ServerName:   msg.GetServerName(),
		DatabaseName: msg.GetDatabaseName(),
		StatementID:  msg.GetStatementId(),
		From:         from.AsTime(),
		To:           to.AsTime(),
		SortKey:      statementSampleSortKey(msg.GetSortColumn()),
		SortDesc:     msg.GetSortDesc(),
		RowLimit:     limit + 1,
		OffsetRows:   resolveOffset(msg.GetOffset()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	rows, hasMore := trimPage(rows, limit)

	samples := make([]*querysheriffv1.StatementSample, len(rows))
	for i, row := range rows {
		samples[i] = statementSampleProto(row)
	}

	return connect.NewResponse(&querysheriffv1.ListStatementSamplesResponse{
		Samples: samples,
		HasMore: hasMore,
	}), nil
}

func statementSampleProto(row clickhouse.Sample) *querysheriffv1.StatementSample {
	return &querysheriffv1.StatementSample{
		Id:         row.ID,
		OccurredAt: timestamppb.New(row.OccurredAt),
		Preview:    sqltext.SamplePreview(row.Query, row.Parameters),
		Tags:       row.Tags,
		HasPlan:    row.HasPlan,
		DurationMs: row.DurationMs,
	}
}

// GetStatementSample returns a sample's query with parameters substituted, and its explain plan.
// Example: "WHERE id=$1", ["42"] -> Query="WHERE id=42", PlanJson="{...}".
func (s *StatementServer) GetStatementSample(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementSampleRequest],
) (*connect.Response[querysheriffv1.GetStatementSampleResponse], error) {
	sample, err := s.authorizedSample(ctx, req.Msg.GetId())
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.GetStatementSampleResponse{
		Query:    sqltext.Concretize(sample.Query, sample.Parameters),
		PlanJson: sample.ExplainPlanJSON,
	}), nil
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

func statementSampleSortKey(col querysheriffv1.SampleSortColumn) string {
	switch col {
	case querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_DURATION:
		return "duration"
	case querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_AT,
		querysheriffv1.SampleSortColumn_SAMPLE_SORT_COLUMN_UNSPECIFIED:
		return "at"
	}

	return "at"
}
