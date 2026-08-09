package server

import (
	"context"
	"sort"
	"strconv"

	"connectrpc.com/connect"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

// A text facet can be effectively unbounded — application_name especially, since correlation
// ids get appended to it ("myapp tx=001IUlJTSH") — so every one is capped. Sorting by count
// first is what makes the cap harmless: a per-request id occurs once, so it sorts last.
const maxLogFacetValues = 200

// The order the columns are passed to grouping() in LogEventFacets. GROUPING() returns one bit
// per argument, set when that column is *not* in the row's grouping set, first argument most
// significant — so a grouping set's id is the all-ones mask with that column's bit cleared.
func logFacetGroupingColumns() []querysheriffv1.LogFacetField {
	return []querysheriffv1.LogFacetField{
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_APPLICATION_NAME,
		querysheriffv1.LogFacetField_LOG_FACET_FIELD_BACKEND_TYPE,
	}
}

func logFacetGroupingID(index, columns int) int32 {
	allSet := int32(1)<<columns - 1

	return allSet & ^(int32(1) << (columns - 1 - index))
}

// The order facets are presented in: what you reach for while debugging first.
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

// ListLogFacets returns, per filterable field, the values present in the window and how often
// they occur — so the picker offers the event types a server really produced rather than all
// ninety-eight it could.
func (s *LogServer) ListLogFacets(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListLogFacetsRequest],
) (*connect.Response[querysheriffv1.ListLogFacetsResponse], error) {
	filter, err := s.resolveLogFilter(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	rows, err := s.queries.LogEventFacets(ctx, filter.facetParams())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.ListLogFacetsResponse{
		Facets: s.buildLogFacets(rows),
	}), nil
}

func (s *LogServer) buildLogFacets(rows []db.LogEventFacetsRow) []*querysheriffv1.LogFacet {
	counts := map[querysheriffv1.LogFacetField]map[string]int64{}
	categories := map[querysheriffv1.LogEvent_LogCategory]int64{}
	classificationCategory := map[string]querysheriffv1.LogEvent_LogCategory{}

	columns := logFacetGroupingColumns()
	byGroupingID := make(map[int32]querysheriffv1.LogFacetField, len(columns))

	for i, field := range columns {
		byGroupingID[logFacetGroupingID(i, len(columns))] = field
	}

	for _, row := range rows {
		field, ok := byGroupingID[row.GroupingID]
		if !ok {
			continue
		}

		value := logFacetValueOf(field, row)
		if counts[field] == nil {
			counts[field] = map[string]int64{}
		}
		counts[field][value] += row.N

		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION {
			category := s.categories.categoryOf(querysheriffv1.LogEvent_LogClassification(row.Classification))
			classificationCategory[value] = category
			categories[category] += row.N
		}
	}

	facets := make([]*querysheriffv1.LogFacet, 0, len(logFacetOrder()))

	for _, field := range logFacetOrder() {
		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY {
			facets = append(facets, s.categoryFacet(categories))

			continue
		}

		// Only classification values carry a category. Passing the lookup for every field would
		// let a database literally named "7" pick up classification 7's category.
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

// categoryFacet lists every category, zero counts included: a dimmed "Lock · 0" answers
// "where did Lock go?" in a way an absent row cannot.
func (s *LogServer) categoryFacet(counts map[querysheriffv1.LogEvent_LogCategory]int64) *querysheriffv1.LogFacet {
	roster := s.categories.all()
	values := make([]*querysheriffv1.LogFacetValue, 0, len(roster)+1)

	for _, category := range roster {
		values = append(values, &querysheriffv1.LogFacetValue{
			Value:    strconv.Itoa(int(category)),
			Count:    counts[category],
			Category: category,
		})
	}

	// Events the collector could not classify have no category, and an unrecognised message can
	// be the interesting one, so they get a bucket rather than being dropped.
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

func logFacetValueOf(field querysheriffv1.LogFacetField, row db.LogEventFacetsRow) string {
	switch field {
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION:
		return strconv.Itoa(int(row.Classification))
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL:
		return strconv.Itoa(int(row.LogLevel))
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE:
		return row.DatabaseName
	case querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME:
		return row.Username
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

// sortedFacetValues orders by count desc then value asc, so the cap keeps what matters and
// equal counts stay stable between requests.
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
