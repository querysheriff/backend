package server

import (
	"context"
	"sort"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/db"
)

type LogServer struct {
	queries    *db.Queries
	notifier   *alerts.Notifier
	categories *logCategories
}

func NewLogServer(queries *db.Queries, notifier *alerts.Notifier) *LogServer {
	return &LogServer{queries: queries, notifier: notifier, categories: newLogCategories()}
}

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

	collectedAt := pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true}

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

func (s *LogServer) QueryLogs(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLogsRequest],
) (*connect.Response[querysheriffv1.QueryLogsResponse], error) {
	msg := req.Msg

	filter, err := s.resolveLogFilter(ctx, msg)
	if err != nil {
		return nil, err
	}

	filter.levels = enumValues(msg.GetLogLevels())

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.queries.ListLogEvents(ctx, filter.listParams(
		logSortKey(msg.GetSortColumn()),
		msg.GetSortDesc(),
		logOrdering{categoryOf: s.categories.orderingArray(), severityOf: logSeverityRanks()},
		limit,
		resolveOffset(msg.GetOffset()),
	))
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
		newSeriesBounds(msg.GetFrom().AsTime(), msg.GetTo().AsTime(), time.Now()),
	)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.QueryLogSeriesResponse{Histogram: histogram}), nil
}

func (s *LogServer) logStatementSamples(
	ctx context.Context,
	rows []db.ListLogEventsRow,
	filter logFilter,
) (map[int64]db.ListLogStatementSamplesRow, error) {
	var ids []int64

	for _, row := range rows {
		if row.StatementSampleID.Valid {
			ids = append(ids, row.StatementSampleID.Int64)
		}
	}

	if len(ids) == 0 {
		return map[int64]db.ListLogStatementSamplesRow{}, nil
	}

	samples, err := s.queries.ListLogStatementSamples(ctx, db.ListLogStatementSamplesParams{
		SampleIds:      ids,
		Since:          filter.since,
		AllowedServers: filter.allowedServers,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	byID := make(map[int64]db.ListLogStatementSamplesRow, len(samples))
	for _, sample := range samples {
		byID[sample.ID] = sample
	}

	return byID, nil
}

func (s *LogServer) logHistogram(
	ctx context.Context,
	filter logFilter,
	bounds seriesBounds,
) (*querysheriffv1.LogHistogram, error) {
	rows, err := s.queries.LogEventHistogram(ctx, filter.histogramParams(bounds))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	ends := bounds.bucketEnds()
	slotOf := make(map[time.Time]int, len(ends))
	for i, end := range ends {
		slotOf[end.UTC()] = i
	}

	perBucketLevel := make([]map[int32]int64, len(ends))
	perBucketClass := make([]map[int32]int64, len(ends))
	levelTotals := map[int32]int64{}
	categoryTotals := map[querysheriffv1.LogEvent_LogCategory]int64{}

	for _, row := range rows {
		idx, ok := slotOf[row.BucketEnd.Time.UTC()]
		if !ok {
			continue
		}

		if perBucketLevel[idx] == nil {
			perBucketLevel[idx] = map[int32]int64{}
			perBucketClass[idx] = map[int32]int64{}
		}

		perBucketLevel[idx][row.LogLevel] += row.N
		perBucketClass[idx][row.Classification] += row.N
		levelTotals[row.LogLevel] += row.N
		categoryTotals[s.categories.categoryOf(
			querysheriffv1.LogEvent_LogClassification(row.Classification),
		)] += row.N
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
		BucketMs:       bounds.bucket.Milliseconds(),
	}, nil
}

func (s *LogServer) categoryBreakdown(classes map[int32]int64) []*querysheriffv1.LogCategoryBreakdown {
	grouped := map[querysheriffv1.LogEvent_LogCategory]map[int32]int64{}

	for classification, count := range classes {
		category := s.categories.categoryOf(querysheriffv1.LogEvent_LogClassification(classification))
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
	row db.ListLogEventsRow,
	samples map[int64]db.ListLogStatementSamplesRow,
) *querysheriffv1.LogRecord {
	classification := querysheriffv1.LogEvent_LogClassification(row.Classification)

	record := &querysheriffv1.LogRecord{
		Id:              row.ID,
		OccurredAt:      protoFromTimestamptz(row.OccurredAt),
		LogLevel:        querysheriffv1.LogEvent_LogLevel(row.LogLevel),
		Classification:  classification,
		Category:        s.categories.categoryOf(classification),
		Pid:             protoFromInt4(row.Pid),
		DatabaseName:    protoFromText(row.DatabaseName),
		Username:        protoFromText(row.Username),
		ApplicationName: protoFromText(row.ApplicationName),
		BackendType:     protoFromText(row.BackendType),
		Message:         protoFromText(row.Message),
		StateCode:       protoFromText(row.StateCode),
		Detail:          protoFromText(row.Detail),
		Hint:            protoFromText(row.Hint),
		Context:         protoFromText(row.Context),
		Statement:       protoFromText(row.Statement),
	}

	if !row.StatementSampleID.Valid {
		return record
	}

	sample, ok := samples[row.StatementSampleID.Int64]
	if !ok {
		return record
	}

	record.StatementSample = &querysheriffv1.LogRecordStatementSample{
		Id:             sample.ID,
		Query:          sample.Query,
		DurationMs:     sample.DurationMs,
		HasExplainPlan: sample.HasExplainPlan.Bool,
		StatementId:    sample.StatementID.Int64,
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
	collectedAt pgtype.Timestamptz,
	events []*querysheriffv1.LogEvent,
	sampleIDs []pgtype.Int8,
) error {
	params := make([]db.InsertLogEventsParams, len(events))
	for i, event := range events {
		params[i] = logEventInsertParams(serverName, collectedAt, event, sampleIDs[i])
	}

	if _, err := s.queries.InsertLogEvents(ctx, params); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	return nil
}

type sampleEntry struct {
	sample         *querysheriffv1.LogStatementSample
	eventIndex     int
	statementIndex int
}

func (s *LogServer) insertStatementSamples(
	ctx context.Context,
	serverName string,
	collectedAt pgtype.Timestamptz,
	events []*querysheriffv1.LogEvent,
) ([]pgtype.Int8, error) {
	sampleIDs := make([]pgtype.Int8, len(events))

	var (
		entries         []sampleEntry
		statementParams []db.EnsureStatementsParams
	)

	for i, event := range events {
		sample := event.GetStatementSample()
		if sample == nil {
			continue
		}

		entry := sampleEntry{sample: sample, eventIndex: i, statementIndex: -1}

		if queryID := event.GetQueryId(); queryID != 0 {
			entry.statementIndex = len(statementParams)
			statementParams = append(statementParams, db.EnsureStatementsParams{
				ServerName:   serverName,
				DatabaseName: event.GetDatabaseName(),
				UserName:     event.GetUsername(),
				QueryID:      queryID,
			})
		}

		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return sampleIDs, nil
	}

	var statementIDs []int64

	if len(statementParams) > 0 {
		ids, err := ensureStatements(ctx, s.queries, statementParams)
		if err != nil {
			return nil, err
		}

		statementIDs = ids
	}

	params := make([]db.InsertStatementSamplesParams, len(entries))
	for i, entry := range entries {
		statementID := pgtype.Int8{}
		if entry.statementIndex >= 0 {
			statementID = pgtype.Int8{Int64: statementIDs[entry.statementIndex], Valid: true}
		}

		param, err := statementSampleInsertParams(serverName, collectedAt, statementID, entry.sample)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		params[i] = param
	}

	var scanErr error

	s.queries.InsertStatementSamples(ctx, params).QueryRow(func(i int, id int64, err error) {
		if err != nil {
			if scanErr == nil {
				scanErr = err
			}

			return
		}

		sampleIDs[entries[i].eventIndex] = pgtype.Int8{Int64: id, Valid: true}
	})

	if scanErr != nil {
		return nil, connect.NewError(connect.CodeInternal, scanErr)
	}

	return sampleIDs, nil
}

func logEventInsertParams(
	serverName string,
	collectedAt pgtype.Timestamptz,
	event *querysheriffv1.LogEvent,
	sampleID pgtype.Int8,
) db.InsertLogEventsParams {
	params := db.InsertLogEventsParams{
		ServerName:        serverName,
		CollectedAt:       collectedAt,
		OccurredAt:        timestamptzFromProto(event.GetOccurredAt()),
		LogLevel:          int32(event.GetLogLevel()),
		Classification:    int32(event.GetClassification()),
		Message:           textFromProto(event.GetMessage()),
		Pid:               int4FromProto(event.GetPid()),
		Username:          textFromProto(event.GetUsername()),
		DatabaseName:      textFromProto(event.GetDatabaseName()),
		ApplicationName:   textFromProto(event.GetApplicationName()),
		Detail:            textFromProto(event.GetDetail()),
		Hint:              textFromProto(event.GetHint()),
		Context:           textFromProto(event.GetContext()),
		Statement:         textFromProto(event.GetStatement()),
		BackendType:       textFromProto(event.GetBackendType()),
		StateCode:         textFromProto(event.GetStateCode()),
		StatementSampleID: sampleID,
	}

	// The sample already holds these bytes.
	if sampleID.Valid {
		params.Message = pgtype.Text{}
		params.Detail = pgtype.Text{}
		params.Statement = pgtype.Text{}
	}

	return params
}

func statementSampleInsertParams(
	serverName string,
	collectedAt pgtype.Timestamptz,
	statementID pgtype.Int8,
	sample *querysheriffv1.LogStatementSample,
) (db.InsertStatementSamplesParams, error) {
	tags, err := jsonbFromStringMap(sample.GetTags())
	if err != nil {
		return db.InsertStatementSamplesParams{}, err
	}

	return db.InsertStatementSamplesParams{
		ServerName:      serverName,
		CollectedAt:     collectedAt,
		OccurredAt:      timestamptzFromProto(sample.GetOccurredAt()),
		StatementID:     statementID,
		Query:           sample.GetQuery(),
		DurationMs:      sample.GetDurationMs(),
		Parameters:      sample.GetParameters(),
		ExplainPlanJson: textFromProto(sample.GetExplainPlanJson()),
		Tags:            tags,
	}, nil
}
