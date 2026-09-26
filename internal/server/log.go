package server

import (
	"context"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/logtaxonomy"
)

type LogServer struct {
	stats      *clickhouse.Client
	notifier   *alerts.Notifier
	categories *logtaxonomy.Categories
}

func NewLogServer(stats *clickhouse.Client, notifier *alerts.Notifier) *LogServer {
	return &LogServer{
		stats:      stats,
		notifier:   notifier,
		categories: logtaxonomy.New(),
	}
}

// ReportLogs stores reported PostgreSQL log events and their statement samples.
// Example: 3 log events -> events/samples stored and crash alert evaluated.
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

	s.notifier.CheckLogs(serverName, events)

	return connect.NewResponse(&querysheriffv1.ReportLogsResponse{}), nil
}

// ListLogs returns filtered log records in time order, paginated.
// Example: levels=[ERROR], newest first, limit=50 -> the latest 50 errors.
func (s *LogServer) ListLogs(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListLogsRequest],
) (*connect.Response[querysheriffv1.ListLogsResponse], error) {
	msg := req.Msg

	filter, err := s.resolveLogFilter(ctx, msg.GetServerName(), msg.GetFrom(), msg.GetTo(), msg.GetFilter())
	if err != nil {
		return nil, err
	}

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.stats.ListLogEvents(ctx, filter, msg.GetSortDesc(), limit+1, resolveOffset(msg.GetOffset()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	rows, hasMore := trimPage(rows, limit)

	samples, err := s.logStatementSamples(ctx, rows, filter)
	if err != nil {
		return nil, err
	}

	records := make([]*querysheriffv1.LogRecord, len(rows))
	for i, row := range rows {
		records[i] = s.logRecordProto(row, samples)
	}

	return connect.NewResponse(&querysheriffv1.ListLogsResponse{
		Records: records,
		HasMore: hasMore,
	}), nil
}

func (s *LogServer) logStatementSamples(
	ctx context.Context,
	rows []clickhouse.LogEvent,
	filter clickhouse.LogFilter,
) (map[uint64]clickhouse.Sample, error) {
	var ids []uint64

	for _, row := range rows {
		if row.StatementSampleID != 0 {
			ids = append(ids, row.StatementSampleID)
		}
	}

	if len(ids) == 0 {
		return map[uint64]clickhouse.Sample{}, nil
	}

	samples, err := s.stats.SamplesByID(ctx, ids, filter.From)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	byID := make(map[uint64]clickhouse.Sample, len(samples))
	for _, sample := range samples {
		byID[sample.ID] = sample
	}

	return byID, nil
}

func (s *LogServer) logRecordProto(
	row clickhouse.LogEvent,
	samples map[uint64]clickhouse.Sample,
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

	if sample, ok := samples[row.StatementSampleID]; ok {
		record.StatementSample = &querysheriffv1.LogRecord_StatementSample{
			Id:          sample.ID,
			StatementId: sample.StatementID,
			Query:       sample.Query,
			DurationMs:  sample.DurationMs,
			HasPlan:     sample.HasPlan,
		}
	}

	return record
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

func (s *LogServer) insertStatementSamples(
	ctx context.Context,
	serverName string,
	collectedAt time.Time,
	events []*querysheriffv1.LogEvent,
) ([]uint64, error) {
	sampleIDs := make([]uint64, len(events))

	var samples []clickhouse.Sample

	for i, event := range events {
		sample := event.GetStatementSample()
		if sample == nil {
			continue
		}

		row := clickhouse.Sample{
			ID: clickhouse.SampleID(serverName, collectedAt, sample.GetQuery(),
				sample.GetDurationMs(), int64(i)),
			CollectedAt:     collectedAt,
			ServerName:      serverName,
			DatabaseName:    event.GetDatabaseName(),
			Query:           sample.GetQuery(),
			DurationMs:      sample.GetDurationMs(),
			Parameters:      sample.GetParameters(),
			ExplainPlanJSON: sample.GetExplainPlanJson(),
			Tags:            sample.GetTags(),
		}

		if queryID := event.GetQueryId(); queryID != 0 {
			row.StatementID = clickhouse.StatementID(
				serverName, event.GetDatabaseName(), event.GetUsername(), queryID)
		}

		if occurred := sample.GetOccurredAt(); occurred != nil {
			row.OccurredAt = occurred.AsTime()
		}

		samples = append(samples, row)
		sampleIDs[i] = row.ID
	}

	if len(samples) == 0 {
		return sampleIDs, nil
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

func (s *LogServer) resolveLogFilter(
	ctx context.Context,
	serverName string,
	from, to *timestamppb.Timestamp,
	filter *querysheriffv1.LogFilter,
) (clickhouse.LogFilter, error) {
	if err := authorizeServerQuery(ctx, serverName, from, to); err != nil {
		return clickhouse.LogFilter{}, err
	}

	return clickhouse.LogFilter{
		ServerName:       serverName,
		From:             from.AsTime(),
		To:               to.AsTime(),
		Levels:           enumValues(filter.GetLevels()),
		Classifications:  s.categories.Selected(enumValues(filter.GetClassifications()), filter.GetCategories()),
		Databases:        filter.GetDatabases(),
		Usernames:        filter.GetUsernames(),
		ApplicationNames: filter.GetApplicationNames(),
		BackendTypes:     filter.GetBackendTypes(),
		Search:           strings.TrimSpace(filter.GetSearch()),
	}, nil
}
