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
	msg := req.Msg

	filter, err := s.resolveLogFilter(ctx, msg.GetServerName(), msg.GetFrom(), msg.GetTo(), msg.GetFilter())
	if err != nil {
		return nil, err
	}

	rows, err := s.stats.LogEventFacets(ctx, filter)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ListLogFacetsResponse{
		Facets: s.buildLogFacets(rows),
	}), nil
}

func (s *LogServer) buildLogFacets(rows []clickhouse.LogFacetRow) []*querysheriffv1.LogFacet {
	facets := map[querysheriffv1.LogFacetField]*querysheriffv1.LogFacet{}
	categories := map[querysheriffv1.LogCategory]int64{}

	for _, row := range rows {
		field := logFacetField(row.Dimension)
		value := &querysheriffv1.LogFacetValue{Value: row.Value, Count: row.Count}

		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION {
			classification, _ := strconv.ParseInt(row.Value, 10, 32)
			value.Category = s.categories.Of(querysheriffv1.LogEvent_LogClassification(classification))
			categories[value.GetCategory()] += row.Count
		}

		facet := facets[field]
		if facet == nil {
			facet = &querysheriffv1.LogFacet{Field: field}
			facets[field] = facet
		}

		if len(facet.GetValues()) == maxLogFacetValues {
			facet.Truncated = true

			continue
		}

		facet.Values = append(facet.Values, value)
	}

	ordered := make([]*querysheriffv1.LogFacet, 0, len(logFacetOrder()))

	for _, field := range logFacetOrder() {
		switch {
		case field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY:
			ordered = append(ordered, s.categoryFacet(categories))
		case facets[field] != nil:
			ordered = append(ordered, facets[field])
		default:
			ordered = append(ordered, &querysheriffv1.LogFacet{Field: field})
		}
	}

	return ordered
}

func (s *LogServer) categoryFacet(counts map[querysheriffv1.LogCategory]int64) *querysheriffv1.LogFacet {
	roster := s.categories.All()
	values := make([]*querysheriffv1.LogFacetValue, 0, len(roster)+1)

	for _, category := range roster {
		values = append(values, &querysheriffv1.LogFacetValue{
			Value:    strconv.Itoa(int(category)),
			Count:    counts[category],
			Category: category,
		})
	}

	if unspecified := counts[querysheriffv1.LogCategory_LOG_CATEGORY_UNSPECIFIED]; unspecified > 0 {
		values = append(values, &querysheriffv1.LogFacetValue{
			Value: strconv.Itoa(int(querysheriffv1.LogCategory_LOG_CATEGORY_UNSPECIFIED)),
			Count: unspecified,
		})
	}

	sort.SliceStable(values, func(i, j int) bool { return values[i].GetCount() > values[j].GetCount() })

	return &querysheriffv1.LogFacet{
		Field:  querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY,
		Values: values,
	}
}
