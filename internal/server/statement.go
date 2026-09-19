package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/sqltext"
)

type StatementServer struct {
	queries *db.Queries
	stats   *clickhouse.Client
	logger  *slog.Logger
}

func NewStatementServer(queries *db.Queries, stats *clickhouse.Client, logger *slog.Logger) *StatementServer {
	return &StatementServer{queries: queries, stats: stats, logger: logger}
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

	var missing []clickhouse.StatementIdentity

	for i, delta := range deltas {
		if !known[statementIDs[i]] {
			missing = append(missing, clickhouse.StatementIdentity{
				UserName:     delta.GetUserName(),
				DatabaseName: delta.GetDatabaseName(),
				QueryID:      delta.GetQueryId(),
			})
		}
	}

	if err = s.storeDeltas(ctx, serverName, collectedAt, deltas, statementIDs, missing); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ReportStatementsResponse{
		UnknownStatements: unknownStatementsProto(missing),
	}), nil
}

func (s *StatementServer) storeDeltas(
	ctx context.Context,
	serverName string,
	collectedAt time.Time,
	deltas []*querysheriffv1.StatementDelta,
	statementIDs []uint64,
	missing []clickhouse.StatementIdentity,
) error {
	unseen := make(map[statementIdentity]bool, len(missing))
	for _, row := range missing {
		unseen[statementIdentity{row.UserName, row.DatabaseName, row.QueryID}] = true
	}

	rows := make([]clickhouse.StatementDelta, len(deltas))

	var mirrored []clickhouse.Statement

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

		identity := statementIdentity{delta.GetUserName(), delta.GetDatabaseName(), delta.GetQueryId()}
		if unseen[identity] {
			mirrored = append(mirrored, clickhouse.Statement{
				ID:           statementIDs[i],
				ServerName:   serverName,
				DatabaseName: delta.GetDatabaseName(),
				UserName:     delta.GetUserName(),
				QueryID:      delta.GetQueryId(),
			})
		}
	}

	if err := s.stats.UpsertStatements(ctx, mirrored); err != nil {
		return err
	}

	return s.stats.InsertStatementDeltas(ctx, rows)
}

type statementIdentity struct {
	userName     string
	databaseName string
	queryID      int64
}

func unknownStatementsProto(rows []clickhouse.StatementIdentity) []*querysheriffv1.StatementIdentity {
	out := make([]*querysheriffv1.StatementIdentity, len(rows))
	for i, row := range rows {
		out[i] = &querysheriffv1.StatementIdentity{
			UserName:     row.UserName,
			DatabaseName: row.DatabaseName,
			QueryId:      row.QueryID,
		}
	}

	return out
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

// QueryStatements returns filtered, sorted, paginated statement statistics.
// Example: server=prod, db=app, limit=50 -> up to 50 matching statements.
func (s *StatementServer) QueryStatements(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryStatementsRequest],
) (*connect.Response[querysheriffv1.QueryStatementsResponse], error) {
	msg := req.Msg

	if err := s.authorizeStatementQuery(
		ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo()); err != nil {
		return nil, err
	}

	serverName := textFilter(msg.GetServerName())
	databaseName := textFilter(msg.GetDatabaseName())

	filter, err := s.resolveStatementFilter(
		ctx, msg.GetQueryText(), msg.GetTagFilters(), serverName, databaseName,
	)
	if err != nil {
		return nil, err
	}

	statements, hasMore, err := s.listStatements(ctx, msg, serverName, databaseName, filter)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.QueryStatementsResponse{
		Statements: statements,
		HasMore:    hasMore,
	}), nil
}

// QueryStatementDetail returns query text, scope, and tags for one statement.
// Example: id=42 -> {Query:"SELECT ...", ServerName:"prod", DatabaseName:"app", Tags:{...}}.
func (s *StatementServer) QueryStatementDetail(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryStatementDetailRequest],
) (*connect.Response[querysheriffv1.QueryStatementDetailResponse], error) {
	msg := req.Msg

	id := msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	if err := s.authorizeStatementID(ctx, id); err != nil {
		return nil, err
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err := requireRange(from, to); err != nil {
		return nil, err
	}

	detail, err := s.stats.StatementDetail(ctx, id)
	if err != nil {
		return nil, statementLookupError(id, err)
	}

	tagsByID, err := s.stats.StatementTags(ctx, []uint64{id})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tags := tagsByID[id]
	if tags == nil {
		tags = map[string]string{}
	}

	return connect.NewResponse(&querysheriffv1.QueryStatementDetailResponse{
		Query:        detail.Query,
		ServerName:   detail.ServerName,
		DatabaseName: detail.DatabaseName,
		Tags:         tags,
	}), nil
}

// QueryStatementSamples returns filtered, sorted, paginated samples for one statement.
// Example: id=42, limit=50 -> up to 50 execution samples.
func (s *StatementServer) QueryStatementSamples(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryStatementSamplesRequest],
) (*connect.Response[querysheriffv1.QueryStatementSamplesResponse], error) {
	msg := req.Msg

	id := msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	if err := s.authorizeStatementID(ctx, id); err != nil {
		return nil, err
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err := requireRange(from, to); err != nil {
		return nil, err
	}

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.stats.ListStatementSamples(ctx, clickhouse.ListSamplesParams{
		StatementID: id,
		From:        from.AsTime(),
		To:          to.AsTime(),
		SortKey:     sampleSortKey(msg.GetSortColumn()),
		SortDesc:    msg.GetSortDesc(),
		RowLimit:    limit + 1,
		OffsetRows:  resolveOffset(msg.GetOffset()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	samples := make([]*querysheriffv1.StatementSample, len(rows))
	for i, row := range rows {
		samples[i] = statementSampleProto(row)
	}

	return connect.NewResponse(&querysheriffv1.QueryStatementSamplesResponse{
		Samples: samples,
		HasMore: hasMore,
	}), nil
}

func statementSampleProto(row clickhouse.Sample) *querysheriffv1.StatementSample {
	sample := &querysheriffv1.StatementSample{
		Id:         row.ID,
		Query:      sqltext.SamplePreview(row.Query, row.Parameters),
		Tags:       row.Tags,
		HasPlan:    row.ExplainPlanJSON != "",
		DurationMs: row.DurationMs,
	}

	if !row.OccurredAt.IsZero() {
		sample.OccurredAt = timestamppb.New(row.OccurredAt)
	}

	return sample
}

// GetStatementSamplePlan returns a sample's concretized query and explain plan.
// Example: "WHERE id=$1", ["42"] -> Query="WHERE id=42", PlanJson="{...}".
func (s *StatementServer) GetStatementSamplePlan(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementSamplePlanRequest],
) (*connect.Response[querysheriffv1.GetStatementSamplePlanResponse], error) {
	sampleID := req.Msg.GetSampleId()
	if sampleID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("sample_id is required"))
	}

	if err := s.authorizeSampleID(ctx, sampleID); err != nil {
		return nil, err
	}

	query, parameters, plan, err := s.stats.SampleText(ctx, sampleID)
	if err != nil {
		return nil, sampleLookupError(sampleID, err)
	}

	return connect.NewResponse(&querysheriffv1.GetStatementSamplePlanResponse{
		Query:    sqltext.Concretize(query, parameters),
		PlanJson: plan,
	}), nil
}

// GetStatementSampleText returns a sample's query with parameters substituted.
// Example: "WHERE id=$1", ["42"] -> "WHERE id=42".
func (s *StatementServer) GetStatementSampleText(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementSampleTextRequest],
) (*connect.Response[querysheriffv1.GetStatementSampleTextResponse], error) {
	sampleID := req.Msg.GetSampleId()
	if sampleID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("sample_id is required"))
	}

	if err := s.authorizeSampleID(ctx, sampleID); err != nil {
		return nil, err
	}

	query, parameters, _, err := s.stats.SampleText(ctx, sampleID)
	if err != nil {
		return nil, sampleLookupError(sampleID, err)
	}

	return connect.NewResponse(&querysheriffv1.GetStatementSampleTextResponse{
		Query: sqltext.Concretize(query, parameters),
	}), nil
}

// GetStatementText returns the full query text for one statement.
// Example: id=42 -> "SELECT * FROM users".
func (s *StatementServer) GetStatementText(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementTextRequest],
) (*connect.Response[querysheriffv1.GetStatementTextResponse], error) {
	id := req.Msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	if err := s.authorizeStatementID(ctx, id); err != nil {
		return nil, err
	}

	query, err := s.stats.StatementText(ctx, id)
	if err != nil {
		return nil, statementLookupError(id, err)
	}

	return connect.NewResponse(&querysheriffv1.GetStatementTextResponse{Query: query}), nil
}
