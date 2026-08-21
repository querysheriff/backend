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

// What QueryLogsRequest and ListLogFacetsRequest share, so a chip narrows the table and the counts
// alike. GetLogLevels is left out on purpose: only the table applies severity, so counts stay stable.
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

// A switch rather than a map so `exhaustive` flags a new column that nothing orders by.
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

type logOrdering struct {
	categoryOf []int32
	severityOf []int32
}

// Least serious first: the enum's own order follows log_min_messages and puts LOG above ERROR.
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
		RowLimit:         limit + 1,
		RowOffset:        offset,
	}
}

func (f logFilter) histogramParams(bounds seriesBounds) db.LogEventHistogramParams {
	return db.LogEventHistogramParams{
		Bucket:           pgtype.Interval{Microseconds: bounds.bucket.Microseconds(), Valid: true},
		Anchor:           pgtype.Timestamptz{Time: bounds.anchor, Valid: true},
		ServerName:       f.serverName,
		AllowedServers:   f.allowedServers,
		Since:            pgtype.Timestamptz{Time: bounds.rangeStart, Valid: true},
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

// The queries read a NULL array as "no filter, every value". An empty slice is not NULL, so it would
// filter on nothing and match no rows.
func emptyToNil(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	return values
}
