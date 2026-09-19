package server

import (
	"context"
	"sort"
	"strconv"

	"connectrpc.com/connect"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
)

const maxLogFacetValues = 200

func logFacetField(dimension clickhouse.LogFacetDimension) querysheriffv1.LogFacetField {
	switch dimension {
	case clickhouse.FacetClassification:
		return querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION
	case clickhouse.FacetLogLevel:
		return querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL
	case clickhouse.FacetDatabaseName:
		return querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE
	case clickhouse.FacetUserName:
		return querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME
	case clickhouse.FacetApplicationName:
		return querysheriffv1.LogFacetField_LOG_FACET_FIELD_APPLICATION_NAME
	case clickhouse.FacetBackendType:
		return querysheriffv1.LogFacetField_LOG_FACET_FIELD_BACKEND_TYPE
	}

	return querysheriffv1.LogFacetField_LOG_FACET_FIELD_UNSPECIFIED
}

func logFacetOrder() []querysheriffv1.LogFacetField {
	return []querysheriffv1.LogFacetField{
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_APPLICATION_NAME,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_BACKEND_TYPE,
	}
}

// ListLogFacets returns available log filter values with counts.
// Example: database -> postgres=120, app=40; level -> ERROR=30, WARNING=20.
func (s *LogServer) ListLogFacets(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListLogFacetsRequest],
) (*connect.Response[querysheriffv1.ListLogFacetsResponse], error) {
	filter, err := s.resolveLogFilter(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	scoped := filter
	scoped.Levels = nil

	rows, err := s.stats.LogEventFacets(ctx, scoped)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ListLogFacetsResponse{
		Facets: s.buildLogFacets(rows),
	}), nil
}

func (s *LogServer) buildLogFacets(rows []clickhouse.LogFacetRow) []*querysheriffv1.LogFacet {
	counts := map[querysheriffv1.LogFacetField]map[string]int64{}
	categories := map[querysheriffv1.LogEvent_LogCategory]int64{}
	classificationCategory := map[string]querysheriffv1.LogEvent_LogCategory{}

	for _, row := range rows {
		field := logFacetField(row.Dimension)

		value := logFacetValueOf(field, row)
		if counts[field] == nil {
			counts[field] = map[string]int64{}
		}
		counts[field][value] += row.Count

		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION {
			category := s.categories.Of(querysheriffv1.LogEvent_LogClassification(row.Classification))
			classificationCategory[value] = category
			categories[category] += row.Count
		}
	}

	facets := make([]*querysheriffv1.LogFacet, 0, len(logFacetOrder()))

	for _, field := range logFacetOrder() {
		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY {
			facets = append(facets, s.categoryFacet(categories))

			continue
		}

		lookup := map[string]querysheriffv1.LogEvent_LogCategory(nil)
		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION {
			lookup = classificationCategory
		}

		values, truncated := sortedFacetValues(counts[field], lookup)
		facets = append(facets, &querysheriffv1.LogFacet{
			Field:     field,
			Values:    values,
			Truncated: truncated,
		})
	}

	return facets
}

func (s *LogServer) categoryFacet(counts map[querysheriffv1.LogEvent_LogCategory]int64) *querysheriffv1.LogFacet {
	roster := s.categories.All()
	values := make([]*querysheriffv1.LogFacetValue, 0, len(roster)+1)

	for _, category := range roster {
		values = append(values, &querysheriffv1.LogFacetValue{
			Value:    strconv.Itoa(int(category)),
			Count:    counts[category],
			Category: category,
		})
	}

	if unspecified := counts[querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED]; unspecified > 0 {
		values = append(values, &querysheriffv1.LogFacetValue{
			Value: strconv.Itoa(int(querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED)),
			Count: unspecified,
		})
	}

	sort.SliceStable(values, func(i, j int) bool { return values[i].GetCount() > values[j].GetCount() })

	return &querysheriffv1.LogFacet{
		Field:  querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY,
		Values: values,
	}
}

func logFacetValueOf(field querysheriffv1.LogFacetField, row clickhouse.LogFacetRow) string {
	switch field {
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION:
		return strconv.Itoa(int(row.Classification))
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL:
		return strconv.Itoa(int(row.LogLevel))
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE:
		return row.DatabaseName
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME:
		return row.UserName
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_APPLICATION_NAME:
		return row.ApplicationName
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_BACKEND_TYPE:
		return row.BackendType
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_UNSPECIFIED:
		return ""
	}

	return ""
}

func sortedFacetValues(
	counts map[string]int64,
	classificationCategory map[string]querysheriffv1.LogEvent_LogCategory,
) ([]*querysheriffv1.LogFacetValue, bool) {
	values := make([]*querysheriffv1.LogFacetValue, 0, len(counts))

	for value, count := range counts {
		values = append(values, &querysheriffv1.LogFacetValue{
			Value:    value,
			Count:    count,
			Category: classificationCategory[value],
		})
	}

	sort.Slice(values, func(i, j int) bool {
		if values[i].GetCount() != values[j].GetCount() {
			return values[i].GetCount() > values[j].GetCount()
		}

		return values[i].GetValue() < values[j].GetValue()
	})

	if len(values) > maxLogFacetValues {
		return values[:maxLogFacetValues], true
	}

	return values, false
}
