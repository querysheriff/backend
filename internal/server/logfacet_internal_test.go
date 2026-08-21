package server

import (
	"testing"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// Read back from Postgres: if the derivation or the column order drifts, values get filed wrong.
func TestLogFacetGroupingIDsMatchPostgres(t *testing.T) {
	t.Parallel()

	// Listed in column order, not by enum: the ids change if grouping() gets more or fewer columns.
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
