package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

// logFilterSource is what QueryLogsRequest and ListLogFacetsRequest have in common, so a chip
// narrows the table and the facet counts the same way. Adding a filter to one request without
// the other stops compiling here.
//
// GetLogLevels is absent on purpose: only the table applies the severity filter, so the facet
// counts stay stable as you toggle it.
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

// logFilter is a resolved, authorized log query: the window plus every filter, ready to become
// params for any of the three log queries.
type logFilter struct {
	serverName       string
	allowedServers   []string
	since            pgtype.Timestamptz
	until            pgtype.Timestamptz
	levels           []int32
	classifications  []int32
	databases        []string
	usernames        []string
	applicationNames []string
	backendTypes     []string
	search           pgtype.Text
}

// resolveLogScope authorizes the caller and validates the window. The heatmaps need nothing
// else; the filtered reads build on it.
func (s *LogServer) resolveLogScope(
	ctx context.Context,
	serverName string,
	from, to *timestamppb.Timestamp,
) (logFilter, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return logFilter{}, err
	}

	if serverName != "" && !principal.CanViewServer(serverName) {
		return logFilter{}, connect.NewError(
			connect.CodePermissionDenied,
			errors.New("access to that server is not allowed"),
		)
	}

	if err = requireRange(from, to); err != nil {
		return logFilter{}, err
	}

	return logFilter{
		serverName:     serverName,
		allowedServers: principal.AllowedServerFilter(),
		since:          timestamptzFromProto(from),
		until:          timestamptzFromProto(to),
	}, nil
}

func (s *LogServer) resolveLogFilter(ctx context.Context, req logFilterSource) (logFilter, error) {
	scope, err := s.resolveLogScope(ctx, req.GetServerName(), req.GetFrom(), req.GetTo())
	if err != nil {
		return logFilter{}, err
	}

	scope.classifications = s.categories.selected(enumValues(req.GetClassifications()), req.GetCategories())
	scope.databases = emptyToNil(req.GetDatabases())
	scope.usernames = emptyToNil(req.GetUsernames())
	scope.applicationNames = emptyToNil(req.GetApplicationNames())
	scope.backendTypes = emptyToNil(req.GetBackendTypes())
	scope.search = textFilter(req.GetFilter())

	return scope, nil
}

// logSortKey maps the proto column onto the `sort_key` the query switches on. A switch rather
// than a map so `exhaustive` flags a new column that nothing orders by.
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

// logOrdering carries the rank arrays the sort indexes into. Both live in Go so the taxonomy
// and the severity order stay in one place rather than being duplicated in SQL.
type logOrdering struct {
	categoryOf []int32
	severityOf []int32
}

// logSeverityRanks ranks the levels by how serious they are to read, least first, so ordering
// by Severity descending surfaces the worst events. Deliberately not the enum's own order,
// which follows `log_min_messages` and puts LOG above ERROR — that would bury a handful of
// errors under thousands of routine LOG lines. Mirrors LEVEL_ROWS in the frontend, reversed.
func logSeverityRanks() []int32 {
	order := []querysheriffv1.LogEvent_LogLevel{
		querysheriffv1.LogEvent_LOG_LEVEL_UNSPECIFIED,
		querysheriffv1.LogEvent_LOG_LEVEL_DEBUG,
		querysheriffv1.LogEvent_LOG_LEVEL_INFO,
		querysheriffv1.LogEvent_LOG_LEVEL_LOG,
		querysheriffv1.LogEvent_LOG_LEVEL_NOTICE,
		querysheriffv1.LogEvent_LOG_LEVEL_WARNING,
		querysheriffv1.LogEvent_LOG_LEVEL_ERROR,
		querysheriffv1.LogEvent_LOG_LEVEL_FATAL,
		querysheriffv1.LogEvent_LOG_LEVEL_PANIC,
	}

	ranks := make([]int32, len(querysheriffv1.LogEvent_LogLevel_name))
	for rank, level := range order {
		ranks[level] = int32(rank)
	}

	return ranks
}

func (f logFilter) listParams(
	sortKey string,
	sortDesc bool,
	ordering logOrdering,
	limit, offset int32,
) db.ListLogEventsParams {
	return db.ListLogEventsParams{
		SortKey:          sortKey,
		SortDesc:         sortDesc,
		CategoryOf:       ordering.categoryOf,
		SeverityOf:       ordering.severityOf,
		ServerName:       f.serverName,
		AllowedServers:   f.allowedServers,
		Since:            f.since,
		Until:            f.until,
		Levels:           f.levels,
		Classifications:  f.classifications,
		Databases:        f.databases,
		Usernames:        f.usernames,
		ApplicationNames: f.applicationNames,
		BackendTypes:     f.backendTypes,
		Search:           f.search,
		// One extra row answers "is there another page" without a second count query.
		RowLimit:  limit + 1,
		RowOffset: offset,
	}
}

func (f logFilter) histogramParams(bucket pgtype.Interval) db.LogEventHistogramParams {
	return db.LogEventHistogramParams{
		Bucket:           bucket,
		ServerName:       f.serverName,
		AllowedServers:   f.allowedServers,
		Since:            f.since,
		Until:            f.until,
		Classifications:  f.classifications,
		Databases:        f.databases,
		Usernames:        f.usernames,
		ApplicationNames: f.applicationNames,
		BackendTypes:     f.backendTypes,
		Search:           f.search,
	}
}

func (f logFilter) facetParams() db.LogEventFacetsParams {
	return db.LogEventFacetsParams{
		ServerName:       f.serverName,
		AllowedServers:   f.allowedServers,
		Since:            f.since,
		Until:            f.until,
		Classifications:  f.classifications,
		Databases:        f.databases,
		Usernames:        f.usernames,
		ApplicationNames: f.applicationNames,
		BackendTypes:     f.backendTypes,
		Search:           f.search,
	}
}

// emptyToNil keeps proto3's "empty repeated field means every value" contract: the queries
// test each array arg for NULL, and a zero-length slice is not NULL.
func emptyToNil(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	return values
}
