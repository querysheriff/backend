package server

import (
	"testing"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// The ids the LogEventFacets rows are routed by. Verified against Postgres by running
// the query's GROUPING SETS and reading back the distinct grouping() values, so if the
// derivation or the column order drifts from the SQL, this fails rather than silently
// filing every value under the wrong facet.
func TestLogFacetGroupingIDsMatchPostgres(t *testing.T) {
	t.Parallel()

	// Kept positional rather than keyed by the enum: CATEGORY is synthesized from the
	// classification counts and is not one of the query's grouping columns. The ids depend on
	// how many columns grouping() takes, so dropping one shifts all of them — these are the
	// six-column values, read back from Postgres.
	want := []struct {
		field querysheriffv1.LogFacetField
		id    int32
	}{
		{querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION, 31},
		{querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL, 47},
		{querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE, 55},
		{querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME, 59},
		{querysheriffv1.LogFacetField_LOG_FACET_FIELD_APPLICATION_NAME, 61},
		{querysheriffv1.LogFacetField_LOG_FACET_FIELD_BACKEND_TYPE, 62},
	}

	columns := logFacetGroupingColumns()
	if len(columns) != len(want) {
		t.Fatalf("grouping columns changed: got %d, want %d", len(columns), len(want))
	}

	for i, expected := range want {
		if columns[i] != expected.field {
			t.Errorf("grouping column %d = %s, want %s", i, columns[i], expected.field)

			continue
		}

		if got := logFacetGroupingID(i, len(columns)); got != expected.id {
			t.Errorf("%s grouping id = %d, want %d", expected.field, got, expected.id)
		}
	}
}

// Every field the picker offers has to be filled in, or that facet silently never
// appears in the response.
func TestLogFacetOrderCoversEveryField(t *testing.T) {
	t.Parallel()

	seen := map[querysheriffv1.LogFacetField]bool{}
	for _, field := range logFacetOrder() {
		seen[field] = true
	}

	for number, name := range querysheriffv1.LogFacetField_name {
		field := querysheriffv1.LogFacetField(number)
		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_UNSPECIFIED {
			continue
		}

		if !seen[field] {
			t.Errorf("%s is missing from logFacetOrder", name)
		}
	}
}
