package server

import (
	"testing"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

// A classification with no category is invisible to the LOGS filter, so this has to fail loudly.
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

// Pin each category's size: a classification moved between them still passes the tests above.
func TestLogCategorySizes(t *testing.T) {
	t.Parallel()

	want := []struct {
		category querysheriffv1.LogEvent_LogCategory
		size     int
	}{
		{querysheriffv1.LogEvent_LOG_CATEGORY_SERVER, 11},
		{querysheriffv1.LogEvent_LOG_CATEGORY_CONNECTION, 15},
		{querysheriffv1.LogEvent_LOG_CATEGORY_WAL_CHECKPOINT, 11},
		{querysheriffv1.LogEvent_LOG_CATEGORY_AUTOVACUUM, 9},
		{querysheriffv1.LogEvent_LOG_CATEGORY_LOCK, 5},
		{querysheriffv1.LogEvent_LOG_CATEGORY_STATEMENT, 5},
		{querysheriffv1.LogEvent_LOG_CATEGORY_STANDBY, 6},
		{querysheriffv1.LogEvent_LOG_CATEGORY_CONSTRAINT_VIOLATION, 5},
		{querysheriffv1.LogEvent_LOG_CATEGORY_APPLICATION_ERROR, 31},
	}

	categories := newLogCategories()

	if len(categories.all()) != len(want) {
		t.Fatalf("got %d categories, want %d", len(categories.all()), len(want))
	}

	for _, expected := range want {
		if got := len(categories.byCategory[expected.category]); got != expected.size {
			t.Errorf("%s has %d classifications, want %d", expected.category, got, expected.size)
		}
	}
}

// A category that expands to nothing makes `selected` return nil, which means "every classification".
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
