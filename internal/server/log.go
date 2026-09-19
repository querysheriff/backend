package server

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/logtaxonomy"
	"github.com/querysheriff/backend/internal/timeseries"
)

type LogServer struct {
	queries    *db.Queries
	stats      *clickhouse.Client
	notifier   *alerts.Notifier
	categories *logtaxonomy.Categories
}

func NewLogServer(queries *db.Queries, stats *clickhouse.Client, notifier *alerts.Notifier) *LogServer {
	return &LogServer{
		queries:    queries,
		stats:      stats,
		notifier:   notifier,
		categories: logtaxonomy.New(),
	}
}

// ReportLogs stores reported PostgreSQL log events and their statement samples.
// Example: 3 log events -> events/samples stored and PANIC alert evaluated.
func (s *LogServer) ReportLogs(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportLogsRequest],
) (*connect.Response[querysheriffv1.ReportLogsResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	events := msg.GetLogEvents()
	if len(events) == 0 {
		return connect.NewResponse(&querysheriffv1.ReportLogsResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	collectedAt := msg.GetCollectedAt().AsTime()

	sampleIDs, err := s.insertStatementSamples(ctx, serverName, collectedAt, events)
	if err != nil {
		return nil, err
	}

	if err = s.insertLogEvents(ctx, serverName, collectedAt, events, sampleIDs); err != nil {
		return nil, err
	}

	s.evaluateAlerts(serverName, events)

	return connect.NewResponse(&querysheriffv1.ReportLogsResponse{}), nil
}

func (s *LogServer) evaluateAlerts(serverName string, events []*querysheriffv1.LogEvent) {
	for _, event := range events {
		if event.GetLogLevel() == querysheriffv1.LogEvent_LOG_LEVEL_PANIC {
			s.notifier.Fire(serverName, alerts.KeyPanic, event.GetMessage())

			return
		}
	}
}

// QueryLogs returns filtered, sorted, paginated log records.
// Example: levels=[ERROR], limit=50 -> up to 50 matching log records.
func (s *LogServer) QueryLogs(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLogsRequest],
) (*connect.Response[querysheriffv1.QueryLogsResponse], error) {
	msg := req.Msg

	filter, err := s.resolveLogFilter(ctx, msg)
	if err != nil {
		return nil, err
	}

	filter.Levels = enumValues(msg.GetLogLevels())

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.stats.ListLogEvents(ctx, filter, clickhouse.LogSortOrder{
		Key:        logSortKey(msg.GetSortColumn()),
		Desc:       msg.GetSortDesc(),
		SeverityOf: logtaxonomy.SeverityRanks(),
		CategoryOf: s.categories.Ranks(),
	}, limit+1, resolveOffset(msg.GetOffset()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	samples, err := s.logStatementSamples(ctx, rows, filter)
	if err != nil {
		return nil, err
	}

	records := make([]*querysheriffv1.LogRecord, len(rows))
	for i, row := range rows {
		records[i] = s.logRecordFromRow(row, samples)
	}

	return connect.NewResponse(&querysheriffv1.QueryLogsResponse{
		Records: records,
		HasMore: hasMore,
	}), nil
}

// QueryLogSeries returns log counts grouped into time buckets, levels, and categories.
// Example: 1m buckets -> 12:01: ERROR=5, WARNING=3.
func (s *LogServer) QueryLogSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLogSeriesRequest],
) (*connect.Response[querysheriffv1.QueryLogSeriesResponse], error) {
	msg := req.Msg

	scope, err := s.resolveLogScope(ctx, msg.GetServerName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	histogram, err := s.logHistogram(
		ctx,
		scope,
		timeseries.NewBounds(msg.GetFrom().AsTime(), msg.GetTo().AsTime(), time.Now()),
	)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.QueryLogSeriesResponse{Histogram: histogram}), nil
}

func (s *LogServer) logStatementSamples(
	ctx context.Context,
	rows []clickhouse.LogEvent,
	filter clickhouse.LogFilter,
) (map[uint64]clickhouse.LogSample, error) {
	var ids []uint64

	for _, row := range rows {
		if row.StatementSampleID != 0 {
			ids = append(ids, row.StatementSampleID)
		}
	}

	if len(ids) == 0 {
		return map[uint64]clickhouse.LogSample{}, nil
	}

	samples, err := s.stats.ListLogStatementSamples(ctx, ids, filter.From)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	byID := make(map[uint64]clickhouse.LogSample, len(samples))
	for _, sample := range samples {
		byID[sample.ID] = sample
	}

	return byID, nil
}

func (s *LogServer) logHistogram(
	ctx context.Context,
	filter clickhouse.LogFilter,
	bounds timeseries.Bounds,
) (*querysheriffv1.LogHistogram, error) {
	scoped := filter
	scoped.From = bounds.RangeStart
	scoped.To = bounds.Anchor
	scoped.Levels = nil

	rows, err := s.stats.LogEventHistogram(ctx, scoped, bounds.Bucket)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	ends := bounds.Ends()
	slotOf := make(map[time.Time]int, len(ends))
	for i, end := range ends {
		slotOf[end.UTC()] = i
	}

	perBucketLevel := make([]map[int32]int64, len(ends))
	perBucketClass := make([]map[int32]int64, len(ends))
	levelTotals := map[int32]int64{}
	categoryTotals := map[querysheriffv1.LogEvent_LogCategory]int64{}

	for _, row := range rows {
		idx, ok := slotOf[row.BucketEnd.UTC()]
		if !ok {
			continue
		}

		if perBucketLevel[idx] == nil {
			perBucketLevel[idx] = map[int32]int64{}
			perBucketClass[idx] = map[int32]int64{}
		}

		perBucketLevel[idx][row.LogLevel] += row.Count
		perBucketClass[idx][row.Classification] += row.Count
		levelTotals[row.LogLevel] += row.Count
		categoryTotals[s.categories.Of(
			querysheriffv1.LogEvent_LogClassification(row.Classification),
		)] += row.Count
	}

	buckets := make([]*querysheriffv1.LogHistogramBucket, len(ends))
	for i, end := range ends {
		buckets[i] = &querysheriffv1.LogHistogramBucket{
			BucketEnd:  timestamppb.New(end),
			Counts:     levelCounts(perBucketLevel[i]),
			Categories: s.categoryBreakdown(perBucketClass[i]),
		}
	}

	return &querysheriffv1.LogHistogram{
		Buckets:        buckets,
		LevelTotals:    levelCounts(levelTotals),
		CategoryTotals: categoryCounts(categoryTotals),
		BucketMs:       bounds.Bucket.Milliseconds(),
	}, nil
}

func (s *LogServer) categoryBreakdown(classes map[int32]int64) []*querysheriffv1.LogCategoryBreakdown {
	grouped := map[querysheriffv1.LogEvent_LogCategory]map[int32]int64{}

	for classification, count := range classes {
		category := s.categories.Of(querysheriffv1.LogEvent_LogClassification(classification))
		if grouped[category] == nil {
			grouped[category] = map[int32]int64{}
		}
		grouped[category][classification] += count
	}

	out := make([]*querysheriffv1.LogCategoryBreakdown, 0, len(grouped))

	for category, classifications := range grouped {
		var total int64
		for _, count := range classifications {
			total += count
		}

		out = append(out, &querysheriffv1.LogCategoryBreakdown{
			Category:        category,
			Count:           total,
			Classifications: classificationCounts(classifications),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GetCategory() < out[j].GetCategory() })

	return out
}

func classificationCounts(counts map[int32]int64) []*querysheriffv1.LogClassificationCount {
	out := make([]*querysheriffv1.LogClassificationCount, 0, len(counts))
	for classification, count := range counts {
		out = append(out, &querysheriffv1.LogClassificationCount{
			Classification: querysheriffv1.LogEvent_LogClassification(classification),
			Count:          count,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].GetCount() != out[j].GetCount() {
			return out[i].GetCount() > out[j].GetCount()
		}

		return out[i].GetClassification() < out[j].GetClassification()
	})

	return out
}

func categoryCounts(counts map[querysheriffv1.LogEvent_LogCategory]int64) []*querysheriffv1.LogCategoryCount {
	out := make([]*querysheriffv1.LogCategoryCount, 0, len(counts))
	for category, count := range counts {
		out = append(out, &querysheriffv1.LogCategoryCount{Category: category, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetCategory() < out[j].GetCategory() })

	return out
}

func levelCounts(counts map[int32]int64) []*querysheriffv1.LogLevelCount {
	out := make([]*querysheriffv1.LogLevelCount, 0, len(counts))
	for level, count := range counts {
		out = append(out, &querysheriffv1.LogLevelCount{
			Level: querysheriffv1.LogEvent_LogLevel(level),
			Count: count,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetLevel() < out[j].GetLevel() })

	return out
}

func (s *LogServer) logRecordFromRow(
	row clickhouse.LogEvent,
	samples map[uint64]clickhouse.LogSample,
) *querysheriffv1.LogRecord {
	classification := querysheriffv1.LogEvent_LogClassification(row.Classification)

	record := &querysheriffv1.LogRecord{
		Id:              row.ID,
		OccurredAt:      timestamppb.New(row.OccurredAt),
		LogLevel:        querysheriffv1.LogEvent_LogLevel(row.LogLevel),
		Classification:  classification,
		Category:        s.categories.Of(classification),
		Pid:             signedPid(row.Pid),
		DatabaseName:    row.DatabaseName,
		Username:        row.UserName,
		ApplicationName: row.ApplicationName,
		BackendType:     row.BackendType,
		Message:         row.Message,
		StateCode:       row.StateCode,
		Detail:          row.Detail,
		Hint:            row.Hint,
		Context:         row.Context,
		Statement:       row.Statement,
	}

	if row.StatementSampleID == 0 {
		return record
	}

	sample, ok := samples[row.StatementSampleID]
	if !ok {
		return record
	}

	record.StatementSample = &querysheriffv1.LogRecordStatementSample{
		Id:             sample.ID,
		Query:          sample.Query,
		DurationMs:     sample.DurationMs,
		HasExplainPlan: sample.HasExplainPlan,
		StatementId:    sample.StatementID,
	}

	return record
}

func enumValues[E ~int32](values []E) []int32 {
	if len(values) == 0 {
		return nil
	}

	out := make([]int32, len(values))
	for i, v := range values {
		out[i] = int32(v)
	}

	return out
}

func (s *LogServer) insertLogEvents(
	ctx context.Context,
	serverName string,
	collectedAt time.Time,
	events []*querysheriffv1.LogEvent,
	sampleIDs []uint64,
) error {
	rows := make([]clickhouse.LogEvent, len(events))
	for i, event := range events {
		rows[i] = logEventRow(serverName, collectedAt, event, sampleIDs[i], int64(i))
	}

	if err := s.stats.InsertLogEvents(ctx, rows); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	return nil
}

type sampleEntry struct {
	statementID    uint64
	databaseName   string
	sample         *querysheriffv1.LogStatementSample
	eventIndex     int
	statementIndex int
}

func (s *LogServer) insertStatementSamples(
	ctx context.Context,
	serverName string,
	collectedAt time.Time,
	events []*querysheriffv1.LogEvent,
) ([]uint64, error) {
	sampleIDs := make([]uint64, len(events))

	var entries []sampleEntry

	for i, event := range events {
		sample := event.GetStatementSample()
		if sample == nil {
			continue
		}

		entry := sampleEntry{
			sample: sample, eventIndex: i, statementIndex: -1,
			databaseName: event.GetDatabaseName(),
		}

		if queryID := event.GetQueryId(); queryID != 0 {
			entry.statementID = clickhouse.StatementID(
				serverName, event.GetDatabaseName(), event.GetUsername(), queryID)
			entry.statementIndex = 0
		}

		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return sampleIDs, nil
	}

	samples := make([]clickhouse.Sample, len(entries))

	for i, entry := range entries {
		id := clickhouse.SampleID(serverName, collectedAt, entry.sample.GetQuery(),
			entry.sample.GetDurationMs(), int64(entry.eventIndex))

		samples[i] = clickhouse.Sample{
			ID:              id,
			CollectedAt:     collectedAt,
			ServerName:      serverName,
			DatabaseName:    entry.databaseName,
			StatementID:     entry.statementID,
			Query:           entry.sample.GetQuery(),
			DurationMs:      entry.sample.GetDurationMs(),
			Parameters:      entry.sample.GetParameters(),
			ExplainPlanJSON: entry.sample.GetExplainPlanJson(),
			Tags:            entry.sample.GetTags(),
		}

		if occurred := entry.sample.GetOccurredAt(); occurred != nil {
			samples[i].OccurredAt = occurred.AsTime()
		}

		sampleIDs[entry.eventIndex] = id
	}

	if err := s.stats.InsertSamples(ctx, samples); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.stats.ReplaceStatementTags(ctx, shapeTagsByStatement(samples)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return sampleIDs, nil
}

func shapeTagsByStatement(samples []clickhouse.Sample) map[uint64]map[string]string {
	byStatement := map[uint64]map[string]string{}

	for _, sample := range samples {
		if sample.StatementID == 0 || len(sample.Tags) == 0 {
			continue
		}

		shape := map[string]string{}

		for key, value := range sample.Tags {
			if !strings.HasSuffix(key, "_id") {
				shape[key] = value
			}
		}

		byStatement[sample.StatementID] = shape
	}

	return byStatement
}

func logEventRow(
	serverName string,
	collectedAt time.Time,
	event *querysheriffv1.LogEvent,
	sampleID uint64,
	index int64,
) clickhouse.LogEvent {
	row := clickhouse.LogEvent{
		ServerName:        serverName,
		CollectedAt:       collectedAt,
		OccurredAt:        collectedAt,
		LogLevel:          int32(event.GetLogLevel()),
		Classification:    int32(event.GetClassification()),
		Message:           event.GetMessage(),
		Pid:               uint32(max(event.GetPid(), 0)),
		UserName:          event.GetUsername(),
		DatabaseName:      event.GetDatabaseName(),
		ApplicationName:   event.GetApplicationName(),
		Detail:            event.GetDetail(),
		Hint:              event.GetHint(),
		Context:           event.GetContext(),
		Statement:         event.GetStatement(),
		BackendType:       event.GetBackendType(),
		StateCode:         event.GetStateCode(),
		StatementSampleID: sampleID,
	}

	if occurred := event.GetOccurredAt(); occurred != nil {
		row.OccurredAt = occurred.AsTime()
	}

	if sampleID != 0 {
		row.Message = ""
		row.Detail = ""
		row.Statement = ""
	}

	row.ID = clickhouse.LogEventID(serverName, collectedAt, row.Pid, event.GetMessage(), index)

	return row
}

type logFilterSource interface {
	GetServerName() string
	GetFrom() *timestamppb.Timestamp
	GetTo() *timestamppb.Timestamp
	GetFilter() string
	GetClassifications() []querysheriffv1.LogEvent_LogClassification
	GetCategories() []querysheriffv1.LogEvent_LogCategory
	GetDatabases() []string
	GetUsernames() []string
	GetApplicationNames() []string
	GetBackendTypes() []string
}

func (s *LogServer) resolveLogScope(
	ctx context.Context,
	serverName string,
	from, to *timestamppb.Timestamp,
) (clickhouse.LogFilter, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return clickhouse.LogFilter{}, err
	}

	if serverName != "" && !principal.CanViewServer(serverName) {
		return clickhouse.LogFilter{}, connect.NewError(
			connect.CodePermissionDenied,
			errors.New("access to that server is not allowed"),
		)
	}

	if err = requireRange(from, to); err != nil {
		return clickhouse.LogFilter{}, err
	}

	return clickhouse.LogFilter{
		ServerName: serverName,
		From:       from.AsTime(),
		To:         to.AsTime(),
	}, nil
}

func (s *LogServer) resolveLogFilter(ctx context.Context, req logFilterSource) (clickhouse.LogFilter, error) {
	scope, err := s.resolveLogScope(ctx, req.GetServerName(), req.GetFrom(), req.GetTo())
	if err != nil {
		return clickhouse.LogFilter{}, err
	}

	scope.Classifications = s.categories.Selected(enumValues(req.GetClassifications()), req.GetCategories())
	scope.Databases = emptyToNil(req.GetDatabases())
	scope.Usernames = emptyToNil(req.GetUsernames())
	scope.ApplicationNames = emptyToNil(req.GetApplicationNames())
	scope.BackendTypes = emptyToNil(req.GetBackendTypes())
	scope.Search = strings.TrimSpace(req.GetFilter())

	return scope, nil
}

func logSortKey(column querysheriffv1.LogSortColumn) string {
	switch column {
	case querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_LEVEL:
		return "level"
	case querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_EVENT:
		return "event"
	case querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_CATEGORY:
		return "category"
	case querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_DATABASE:
		return "database"
	case querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_USERNAME:
		return "user"
	case querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_AT,
		querysheriffv1.LogSortColumn_LOG_SORT_COLUMN_UNSPECIFIED:
		return "at"
	}

	return "at"
}

func emptyToNil(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	return values
}
