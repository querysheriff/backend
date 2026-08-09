package server

import (
	"testing"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// A classification with no category is invisible to the LOGS category filter, so
// adding one to the proto without adding it here has to fail loudly.
func TestLogCategoriesCoverEveryClassification(t *testing.T) {
	t.Parallel()

	categories := newLogCategories()
	values := querysheriffv1.LogEvent_LogClassification_name

	for number, name := range values {
		classification := querysheriffv1.LogEvent_LogClassification(number)
		if classification == querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED {
			continue
		}

		if got := categories.categoryOf(classification); got == querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED {
			t.Errorf("%s has no category; add it to a group in logcategory.go", name)
		}
	}

	// -1 for UNSPECIFIED, which is deliberately uncategorised.
	if want := len(values) - 1; len(categories.byClassification) != want {
		t.Errorf("mapped %d classifications, want %d", len(categories.byClassification), want)
	}
}

// Each classification belongs to exactly one category, so the category filter
// cannot double-count an event.
func TestLogCategoryGroupsDoNotOverlap(t *testing.T) {
	t.Parallel()

	seen := map[querysheriffv1.LogEvent_LogClassification]querysheriffv1.LogEvent_LogCategory{}

	for _, group := range append(serverLogCategoryGroups(), statementLogCategoryGroups()...) {
		for _, classification := range group.classifications {
			if first, ok := seen[classification]; ok {
				t.Errorf("%s is in both %s and %s", classification, first, group.category)

				continue
			}

			seen[classification] = group.category
		}
	}
}

// The membership was transcribed from pganalyze's Log Insights docs, so pin the size of
// each category against the code range it mirrors. A classification moved between
// categories still passes the coverage tests above; this one catches it.
func TestLogCategorySizesMatchPganalyze(t *testing.T) {
	t.Parallel()

	want := []struct {
		category querysheriffv1.LogEvent_LogCategory
		codes    string
		size     int
	}{
		{querysheriffv1.LogEvent_LOG_CATEGORY_SERVER, "S1-S11", 11},
		// C20-C33 is fourteen codes; ours splits Postgres 14's "connection authenticated"
		// out of C21, so fifteen.
		{querysheriffv1.LogEvent_LOG_CATEGORY_CONNECTION, "C20-C33", 15},
		{querysheriffv1.LogEvent_LOG_CATEGORY_WAL_CHECKPOINT, "W40-W46, W50-W53", 11},
		{querysheriffv1.LogEvent_LOG_CATEGORY_AUTOVACUUM, "A60-A68", 9},
		{querysheriffv1.LogEvent_LOG_CATEGORY_LOCK, "L70-L74", 5},
		{querysheriffv1.LogEvent_LOG_CATEGORY_STATEMENT, "T80-T84", 5},
		{querysheriffv1.LogEvent_LOG_CATEGORY_STANDBY, "B90-B95", 6},
		{querysheriffv1.LogEvent_LOG_CATEGORY_CONSTRAINT_VIOLATION, "V100-V104", 5},
		{querysheriffv1.LogEvent_LOG_CATEGORY_APPLICATION_ERROR, "U110-U140", 31},
	}

	categories := newLogCategories()

	if len(categories.all()) != len(want) {
		t.Fatalf("got %d categories, want pganalyze's %d", len(categories.all()), len(want))
	}

	for _, expected := range want {
		if got := len(categories.byCategory[expected.category]); got != expected.size {
			t.Errorf("%s (%s) has %d classifications, want %d",
				expected.category, expected.codes, got, expected.size)
		}
	}
}

// A category that expands to nothing is the worst possible failure here: `selected` returns
// nil, the queries read nil as "every classification", and the filter silently shows
// everything instead of narrowing. Uncategorized did exactly that, because it is not one of
// the declared groups. Cover every category the picker can offer, not just that one.
func TestEveryCategorySelectsSomething(t *testing.T) {
	t.Parallel()

	categories := newLogCategories()

	offerable := append([]querysheriffv1.LogEvent_LogCategory{
		querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED,
	}, categories.all()...)

	for _, category := range offerable {
		selected := categories.selected(nil, []querysheriffv1.LogEvent_LogCategory{category})
		if len(selected) == 0 {
			t.Errorf("selecting %s expands to no classifications, so it would match everything", category)
		}
	}
}

// Uncategorized means exactly the one classification with no category, not "anything".
func TestUncategorizedSelectsOnlyTheUnclassified(t *testing.T) {
	t.Parallel()

	categories := newLogCategories()
	got := categories.selected(nil, []querysheriffv1.LogEvent_LogCategory{
		querysheriffv1.LogEvent_LOG_CATEGORY_UNSPECIFIED,
	})

	want := int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_UNSPECIFIED)
	if len(got) != 1 || got[0] != want {
		t.Errorf("Uncategorized selected %v, want exactly [%d]", got, want)
	}
}

func TestLogCategoriesExpand(t *testing.T) {
	t.Parallel()

	categories := newLogCategories()

	if got := categories.expand(nil); got != nil {
		t.Errorf("no categories should expand to nil (meaning every classification), got %v", got)
	}

	got := categories.expand([]querysheriffv1.LogEvent_LogCategory{querysheriffv1.LogEvent_LOG_CATEGORY_LOCK})
	want := []int32{
		int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_ACQUIRED),
		int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_WAITING),
		int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_TIMEOUT),
		int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_DETECTED),
		int32(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_AVOIDED),
	}

	if len(got) != len(want) {
		t.Fatalf("expand(LOCK) returned %d classifications, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("expand(LOCK)[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}
